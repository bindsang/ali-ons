'use strict';

// add by zhangbing

const ConsumeConcurrentlyStatus = require('./consume-concurrently-status');
const MessageModel = require('../protocol/message_model');
const CMResult = require('../protocol/cm_result');
const MessageConst = require('../message/message_const');
const MessageQueue = require('../message_queue');
const MixAll = require('../mix_all');

module.exports = class ConsumeMessageConcurrentlyService {
  /**
   * @param {MQPushConsumer} consumer -
   * @param {Function} messageListener -
   */
  constructor(consumer, messageListener) {
    this._defaultMQPushConsumer = consumer;
    this._messageListener = messageListener;
    this._logger = this._defaultMQPushConsumer.logger;
    this.consumerGroup = consumer.consumerGroup;
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

  async consumeMessageDirectly(msg, brokerName) {
    const result = {
      order: false,
      autoCommit: true,
      consumeResult: null,
      remark: null,
      spentTimeMills: 0,
    };

    const messageQueue = new MessageQueue(msg.topic, brokerName, msg.queueId);
    this.resetRetryTopic(msg);
    const processQueue = this.__defaultMQPushConsumer.processQueueTable.get(messageQueue.key);

    const beginTime = Date.now();

    this._logger.info(`consumeMessageDirectly receive new message: ${msg.msgId}`);
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

      this._logger.warn(
        `consumeMessageDirectly exception: ${result.remark} Group: ${this.consumerGroup} Msgs: ${msg.msgId} MQ: ${messageQueue.key}`,
        err);
    }

    result.spentTimeMills = Date.now() - beginTime;

    this._logger.info(`consumeMessageDirectly Result: ${JSON.stringify(result)}`);
    return result;
  }

  resetRetryTopic(msg) {
    const groupTopic = MixAll.getRetryTopic(this.consumerGroup);
    const retryTopic = msg.properties[MessageConst.PROPERTY_RETRY_TOPIC];
    if (retryTopic && groupTopic === msg.topic) {
      msg.topic = retryTopic;
    }
  }

  /**
   * @param {Message} msg -
   * @param {ProcessQueue} processQueue -
   * @param {MessageQueue} messageQueue -
   */
  async submitConsumeRequest(msg, processQueue, messageQueue) {
    if (processQueue.dropped) {
      this._logger.info(`the message queue not be able to consume, because it's dropped. group=${this.consumerGroup} ${this.messageQueue.key}`);
      return;
    }
    /**
     * @type {string}
     */
    let status;
    try {
      status = await this._messageListener(msg, messageQueue, processQueue);
    } catch (err) {
      err.message = `process mq message failed, topic: ${msg.topic}, msgId: ${msg.msgId}, ${err.message}`;
      this._logger.warn(
        `concurrently message exception: ${err.message}, Group: ${this.consumerGroup}, MQ: ${messageQueue.key}`,
        err);
      this._defaultMQPushConsumer.emit('error', err);
    }

    if (status !== ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
      status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
      this._logger.warn(`consumeMessage Concurrently return not OK, status: ${status}, Group: ${this.consumerGroup} Msgs: ${msg.msgId} MQ: ${messageQueue.key}`);
    }

    if (!processQueue.dropped) {
      await this._processConsumeResult(status, msg, processQueue, messageQueue);
    } else {
      this._logger.warn('processQueue is dropped without process consume result. messageQueue=%s, msg=%s', messageQueue.key, msg.msgId);
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
          this._logger.warn(`[MQPushConsumer] BROADCASTING consume message failed, drop it, msgId: ${msg.msgId}`);
          break;
        case MessageModel.CLUSTERING:
          {
            // 把消息再还回到消息队列服务中去，等待下一次消息队列服务再拉起消息处理
            // 如果还回去失败了，则在本地延时重试
            const result = await this._sendMessageBack(msg, processQueue);
            if (!result) {
              // 本地重试情况下需要给 reconsumeTimes + 1
              msg.reconsumeTimes++;
              this._submitConsumeRequestLater(msg, processQueue, messageQueue);
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
      this._defaultMQPushConsumer.emit('error', err);
      this._logger.error(`[MQPushConsumer] sendMessageBack exception, group: ${this.consumerGroup} msg: ${msg.msgId}`, err);
    }

    return false;
  }

  // 此处不需要像顺序队列的实现那样保证顺序，所以可以不用async
  _submitConsumeRequestLater(msg, processQueue, messageQueue) {
    setTimeout(() => this.submitConsumeRequest(msg, processQueue, messageQueue), 5000);
  }
};
