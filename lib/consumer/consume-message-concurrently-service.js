'use strict';

// add by zhangbing

const ConsumeConcurrentlyStatus = require('./consume-concurrently-status');
const MessageModel = require('../protocol/message_model');
const CMResult = require('../protocol/cm_result');
const MessageConst = require('../message/message_const');
const MessageQueue = require('../message_queue');
const MixAll = require('../mix_all');
const { ResourcePoolExecutor, RejectedError } = require('../ext/resource-pool-executor');

class ConsumeRequest {
  /**
   * @param {ConsumeMessageConcurrentlyService} service -
   * @param {Message} msg -
   * @param {ProcessQueue} processQueue -
   * @param {MessageQueue} messageQueue -
   */
  constructor(service, msg, processQueue, messageQueue) {
    this._service = service;
    this._msg = msg;
    this._processQueue = processQueue;
    this._messageQueue = messageQueue;
  }
  get msg() {
    return this._msg;
  }

  get processQueue() {
    return this._processQueue;
  }

  get messageQueue() {
    return this._messageQueue;
  }

  async run() {
    const listener = this._service._messageListener;
    const logger = this._service.logger;

    const msg = this._msg;
    const messageQueue = this._messageQueue;
    const processQueue = this._processQueue;

    if (processQueue.dropped) {
      logger.info(`the message queue not be able to consume, because it's dropped. group=${this.consumerGroup} ${this.messageQueue.key}`);
      return;
    }

    /**
     * @type {string}
     */
    let status;
    try {
      this._service._resetRetryTopic(msg);
      if (msg) {
        msg.properties[MessageConst.PROPERTY_CONSUME_START_TIMESTAMP] = String(Date.now());
      }

      status = await listener(msg, messageQueue, processQueue);
    } catch (err) {
      err.message = `process mq message failed, topic: ${msg.topic}, msgId: ${msg.msgId}, ${err.message}`;
      this.service._defaultMQPushConsumer.emit('error', err);
    }

    if (!status) {
      status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
    }

    if (!processQueue.dropped) {
      await this._service._processConsumeResult(status, msg, processQueue, messageQueue);
    } else {
      logger.warn('processQueue is dropped without process consume result. messageQueue=%s, msg=%s', messageQueue.key, msg.msgId);
    }
  }
}

class ConsumeMessageConcurrentlyService {
  /**
   * @param {MQPushConsumer} consumer -
   * @param {Function} messageListener -
   */
  constructor(consumer, messageListener) {
    this._defaultMQPushConsumer = consumer;
    this._messageListener = messageListener;
    this.logger = this._defaultMQPushConsumer.logger;
    this.consumerGroup = consumer.consumerGroup;
    this._consumeExecutor = new ResourcePoolExecutor(consumer.consumeConcurrentMin);
  }

  init() {
    const clearInterval = this._defaultMQPushConsumer.consumeTimeout * 60 * 1000;
    this._clearExpiresHandler = setInterval(() => this._cleanExpireMsg(), clearInterval);
  }

  close() {
    if (this._clearExpiresHandler) {
      clearInterval(this._clearExpiresHandler);
    }
  }

  async _cleanExpireMsg() {
    for (const [ , item ] of this._defaultMQPushConsumer._processQueueTable) {
      const processQueue = item.processQueue;
      await processQueue.cleanExpiredMsg(this._defaultMQPushConsumer);
    }
  }

  get corePoolSize() {
    return this.consumeExecutor.corePoolSize;
  }

  async consumeMessageDirectly(msg, brokerName) {
    const result = {
      order: false,
      autoCommit: true,
      consumeResult: null,
      remark: null,
      spentTimeMills: 0,
    };

    const messageQueue = new MessageQueue(msg.topic, brokerName, msg.queueId);
    this._resetRetryTopic(msg);
    const processQueue = this._defaultMQPushConsumer.processQueueTable.get(messageQueue.key);

    const beginTime = Date.now();

    this.logger.info(`consumeMessageDirectly receive new message: ${msg.msgId}`);
    try {
      const status = await this._messageListener(msg, messageQueue, processQueue);
      if (status) {
        switch (status) {
          case ConsumeConcurrentlyStatus.CONSUME_SUCCESS:
            result.consumeResult = CMResult.CR_SUCCESS;
            break;
          case ConsumeConcurrentlyStatus.RECONSUME_LATER:
            result.consumeResult = CMResult.CR_LATER;
            break;
          default:
            break;
        }
      } else {
        result.consumeResult = CMResult.CR_RETURN_NULL;
      }
    } catch (err) {
      result.consumeResult = CMResult.CR_THROW_EXCEPTION;
      result.remark = err.message + ' -> ' + err.statck;

      this.logger.warn(
        `consumeMessageDirectly exception: ${result.remark} Group: ${this.consumerGroup} Msgs: ${msg.msgId} MQ: ${messageQueue.key}`,
        err);
    }

    result.spentTimeMills = Date.now() - beginTime;

    this.logger.info(`consumeMessageDirectly Result: ${JSON.stringify(result)}`);
    return result;
  }

  _resetRetryTopic(msg) {
    const groupTopic = MixAll.getRetryTopic(this.consumerGroup);
    const retryTopic = msg.properties[MessageConst.PROPERTY_RETRY_TOPIC];
    if (retryTopic && groupTopic === msg.topic) {
      msg.topic = retryTopic;
    }
  }

  /**
   * @param {Message[]} msgs -
   * @param {ProcessQueue} processQueue -
   * @param {MessageQueue} messageQueue -
   */
  submitConsumeRequest(msgs, processQueue, messageQueue) {
    for (const msg of msgs) {
      const consumeRequest = new ConsumeRequest(this, msg, processQueue, messageQueue);
      try {
        this._consumeExecutor.submit(consumeRequest);
      } catch (err) {
        if (err instanceof RejectedError) {
          this._submitConsumeRequestLater1(consumeRequest);
        } else {
          throw err;
        }
      }
    }
  }

  /**
   *
   * @param {string} status -
   * @param {any} msg -
   * @param {ProcessQueue} processQueue -
   * @param {MessageQueue} messageQueue -
   * @return {Promise<boolean>} -
   */
  async _processConsumeResult(status, msg, processQueue, messageQueue) {
    if (!msg) {
      return;
    }

    if (status === ConsumeConcurrentlyStatus.RECONSUME_LATER) {
      // 消息处理出错，处理重试
      switch (this._defaultMQPushConsumer.messageModel) {
        case MessageModel.BROADCASTING:
          this.logger.warn(`[MQPushConsumer] BROADCASTING consume message failed, drop it, msgId: ${msg.msgId}`);
          break;
        case MessageModel.CLUSTERING:
          {
            // 把消息再还回到消息队列服务中去，等待下一次消息队列服务再拉起消息处理
            // 如果还回去失败了，则在本地延时重试
            const result = await this._sendMessageBack(msg, processQueue);
            if (!result) {
              msg.reconsumeTimes++;
              this._submitConsumeRequestLater0(msg, processQueue, messageQueue);
              // 如果sendBack出错，把msg设置为null，目的是后面remove方法不执行移除
              msg = null;
            }
          }
          break;
        default:
          break;
      }
    }

    const offset = processQueue.remove(msg);
    if (offset >= 0 && !processQueue.dropped) {
      this._defaultMQPushConsumer._offsetStore.updateOffset(messageQueue, offset, true);
    }
  }

  /**
   *
   * @param {Message} msg -
   * @param {ProcessQueue} processQueue -
   * @return {Promise<boolean>} -
   */
  async _sendMessageBack(msg, processQueue) {
    /**
     * Message consume retry strategy<br>
     * -1,no retry,put into DLQ directly<br>
     * 0,broker control retry frequency<br>
     * >0,client control retry frequency
     */
    const delayLevel = 0;
    try {
      await this._defaultMQPushConsumer.sendMessageBack(msg, delayLevel, processQueue.brokerName);
      return true;
    } catch (err) {
      this.logger.error(`sendMessageBack exception, group: ${this.consumerGroup} msg: ${msg.msgId}`, err);
    }

    return false;
  }

  // 此处不需要像顺序队列的实现那样保证顺序，所以可以不用async
  _submitConsumeRequestLater0(msg, processQueue, messageQueue) {
    setTimeout(() => this.submitConsumeRequest(msg, processQueue, messageQueue), 5000);
  }

  _submitConsumeRequestLater1(consumeRequest) {
    setTimeout(() => this._consumeExecutor.submit(consumeRequest), 5000);
  }
}

module.exports = ConsumeMessageConcurrentlyService;
