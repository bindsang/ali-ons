'use strict';

// add by zhangbing

const ConsumeConcurrentlyStatus = require('./consume-concurrently-status');
const MessageModel = require('../protocol/message_model');

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

  start() {
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

  /**
   * @param {Message} msg -
   * @param {ProcessQueue} processQueue -
   * @param {MessageQueue} messageQueue -
   */
  async submitConsumeRequest(msg, processQueue, messageQueue) {
    /**
     * @type {string}
     */
    let status;
    try {
      status = await this._messageListener(msg, messageQueue, processQueue);
    } catch (err) {
      err.message = `process mq message failed, topic: ${msg.topic}, msgId: ${msg.msgId}, ${err.message}`;
      this.emit('error', err);
    }

    if (!status) {
      status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
    }

    if (!processQueue.dropped) {
      await this._processConsumeResult(status, processQueue, messageQueue, this);
    } else {
      this.logger.warn('processQueue is dropped without process consume result. messageQueue=%s, msg=%s', messageQueue.key, msg.msgId);
    }
  }

  /**
   *
   * @param {any} msg -
   * @param {string} status -
   * @param {ProcessQueue} processQueue -
   * @param {MessageQueue} messageQueue -
   * @return {Promise<boolean>} -
   */
  async _processConsumeResult(msg, status, processQueue, messageQueue) {
    if (!msg) {
      return;
    }

    if (status === ConsumeConcurrentlyStatus.RECONSUME_LATER) {
      // 消息处理出错，处理重试
      switch (this.messageModel) {
        case MessageModel.BROADCASTING:
          this.logger.warn('[MQPushConsumer] BROADCASTING consume message failed, drop it, msgId: %s', msg.msgId);
          break;
        case MessageModel.CLUSTERING:
          {
            // 把消息再还回到消息队列服务中去，等待下一次消息队列服务再拉起消息处理
            // 如果还回去失败了，则在本地延时重试
            const result = await this._sendMessageBack(msg, processQueue);
            if (!result) {
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

    if (!msg) {
      // msg 为null时表示sendMessageBack失败，不再执行移除和更新偏移位置的操作
      return;
    }

    const offset = processQueue.remove(msg);
    if (offset >= 0 && !processQueue.dropped) {
      this._defaultMQPushConsumer._offsetStore.updateOffset(processQueue, offset, true);
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
      this.logger.error('sendMessageBack exception, group: ' + this.option.consumerGroup + ' msg: ' + msg.msgId, err);
    }

    return false;
  }

  // 此处不需要像顺序队列的实现那样保证顺序，所以可以不用async
  _submitConsumeRequestLater(msg, processQueue, messageQueue) {
    setTimeout(() => this.submitConsumeRequest(msg, processQueue, messageQueue), 5000);
  }
};
