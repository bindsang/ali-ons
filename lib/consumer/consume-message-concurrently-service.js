'use strict';

const { ConsumeConcurrentlyStatus } = require('./consumer-status');
const MessageModel = require('../protocol/message_model');
const MessageConst = require('../message/message_const');
const Message = require('../message/message');
const MixAll = require('../mix_all');

module.exports = class ConsumeMessageConcurrentlyService {
  /**
   * @param {MQConsumer} consumer -
   * @param {Function} messageListener -
   */
  constructor(consumer, messageListener) {
    this._defaultMQPushConsumer = consumer;
    this._messageListener = messageListener;
    this.consumerGroup = consumer.consumerGroup;
  }

  /**
   * @param {Message} msg -
   * @param {ProcessQueue} pq -
   * @param {MessageQueue} mq -
   */
  async submitConsumeRequest(msg, pq, mq) {
    let status;
    try {
      status = await this._messageListener(msg, mq, pq);
    } catch (err) {
      err.message = `process mq message failed, topic: ${msg.topic}, msgId: ${msg.msgId}, ${err.message}`;
      this.emit('error', err);
    }

    if (!status) {
      status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
    }

    if (!pq.dropped) {
      await this._processConsumeResult(status, context, this);
    } else {
      this.logger.warn('processQueue is dropped without process consume result. messageQueue=%s, msg=%s', mq.key, msg.msgId);
    }
  }

  /**
   *
   * @param {any} msg -
   * @param {ConsumeConcurrentlyStatus} status -
   * @param {ProcessQueue} processQueue -
   * @return {Promise<boolean>} -
   */
  async _processConsumeResult(msg, status, processQueue) {
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
              this._submitConsumeRequestLater(msg, processQueue);
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
    const brokerName = processQueue.brokerName;

    try {
      try {
        const brokerAddr = brokerName ? this._mqClient.findBrokerAddressInPublish(brokerName) : msg.storeHost;
        await this._mqClient.consumerSendMessageBack(brokerAddr, msg,
          this.option.consumerGroup, delayLevel, 5000, this.options.maxReconsumeTimes);
      } catch (err) {
        // 出现错误尝试使用内置的producer发送
        this.logger.error('sendMessageBack Exception, ' + this.option.consumerGroup, err);
        const newMsg = new Message(MixAll.getRetryTopic(this.option.consumerGroup), msg.body);

        const originMsgId = msg.properties[MessageConst.PROPERTY_ORIGIN_MESSAGE_ID];
        newMsg.properties[MessageConst.PROPERTY_ORIGIN_MESSAGE_ID] = !originMsgId ? msg.msgId : originMsgId;
        newMsg.setFlag(msg.getFlag());
        newMsg.properties = msg.properties;
        newMsg.properties[MessageConst.PROPERTY_RETRY_TOPIC] = msg.topic;
        newMsg.properties[MessageConst.PROPERTY_RECONSUME_TIME] = String(msg.reconsumeTimes + 1);
        newMsg.properties[MessageConst.PROPERTY_MAX_RECONSUME_TIMES] = String(this.options.maxReconsumeTimes);
        newMsg.delayTimeLevel = 3 + msg.reconsumeTimes;
        await this._mqClient.getDefaultMQProducer().send(newMsg);
      }
      return true;
    } catch (err) {
      this.logger.error('sendMessageBack exception, group: ' + this.option.consumerGroup + ' msg: ' + msg.msgId, err);
    }

    return false;
  }

  _submitConsumeRequestLater(msg, processQueue, messageQueue) {
    setTimeout(() => this.submitConsumeRequest(msg, processQueue, messageQueue), 5000);
  }
};
