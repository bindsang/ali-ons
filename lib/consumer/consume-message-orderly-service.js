'use strict';

// add by zhangbing

const ConsumeOrderlyStatus = require('./consume-orderly-status');
const MessageModel = require('../protocol/message_model');
const ProcessQueue = require('../process_queue');
const MessageConst = require('../message/message_const');
const MixAll = require('../mix_all');
const Message = require('..').Message;
const sleep = require('mz-modules/sleep');

const MAX_TIME_CONSUME_CONTINUOUSLY = 60000;

class ConsumeMessageOrderlyService {
  /**
   * @param {MQPushConsumer} consumer -
   * @param {Function} messageListener -
   */
  constructor(consumer, messageListener) {
    this._defaultMQPushConsumer = consumer;
    this._messageListener = messageListener;
    this.logger = this._defaultMQPushConsumer.logger;
    this.consumerGroup = consumer.consumerGroup;
    this.consumeRequestQueue = [];
  }

  init() {
    if (this._defaultMQPushConsumer.messageModel === MessageModel.CLUSTERING) {
      setTimeout(async () => {
        await this.lockMQPeriodically();
        setInterval(() => this.lockMQPeriodically(), ProcessQueue.REBALANCE_LOCK_INTERVAL);
      }, 1000);
    }
  }

  async close() {
    this.stopped = true;
    if (this._defaultMQPushConsumer.messageModel === MessageModel.CLUSTERING) {
      await this.unlockAllMQ();
    }
  }

  async unlockAllMQ() {
    await this._defaultMQPushConsumer.unlockAll(false);
  }

  /**
   * @param {Message} msg -
   * @param {ProcessQueue} processQueue -
   * @param {MessageQueue} messageQueue -
   */
  async submitConsumeRequest(msg, processQueue, messageQueue) {
    if (processQueue.dropped) {
      this.logger.warn(`run, the message queue not be able to consume, because it's dropped. ${messageQueue.key}`);
      return;
    }

    if (this._defaultMQPushConsumer.messageModel === MessageModel.BROADCASTING
      || (processQueue.locked && !processQueue.lockExpired)) {
      const beginTime = Date.now();
      let continueConsume = true;
      while (continueConsume) {
        if (processQueue.dropped) {
          this.logger.warn(`the message queue not be able to consume, because it's dropped. ${messageQueue.key}`);
          break;
        }

        if (this._defaultMQPushConsumer.messageModel === MessageModel.CLUSTERING
          && !processQueue.locked) {
          this.logger.warn(`the message queue not locked, so consume later, ${messageQueue.key}`);
          await this._tryLockLaterAndReconsume(messageQueue, processQueue, 10);
          break;
        }

        if (this._defaultMQPushConsumer.messageModel === MessageModel.CLUSTERING
          && processQueue.lockExpired) {
          this.logger.warn(`the message queue lock expired, so consume later, ${messageQueue.key}`);
          await this._tryLockLaterAndReconsume(messageQueue, processQueue, 10);
          break;
        }

        const interval = Date.now() - beginTime;
        if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
          await this._submitConsumeRequestLater(processQueue, messageQueue, 10);
          break;
        }

        const msg = processQueue.takeMessage();
        if (msg) {
          /**
           * @type {string}
           */
          let status;
          try {
            if (processQueue.dropped) {
              this.logger.warn(`consumeMessage, the message queue not be able to consume, because it's dropped. ${messageQueue.key}`);
              break;
            }

            status = await this._messageListener(msg, messageQueue, processQueue);
          } catch (err) {
            err.message = `process mq message failed, topic: ${msg.topic}, msgId: ${msg.msgId}, ${err.message}`;
            this._defaultMQPushConsumer.emit('error', err);
          }

          if (!status || status === ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT) {
            this.logger.warn(`consumeMessage Orderly return not OK, Group: ${this.consumerGroup} Msgs: ${msg.msgId} MQ: ${messageQueue.key}`);
          }

          if (!status) {
            status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
          }

          continueConsume = await this._processConsumeResult(status, msg, processQueue, messageQueue, this);
        } else {
          continueConsume = false;
        }
      }
    } else {
      if (processQueue.dropped) {
        this.logger.warn(`the message queue not be able to consume, because it's dropped. ${messageQueue.key}`);
        return;
      }

      await this._tryLockLaterAndReconsume(messageQueue, processQueue, 100);
    }
  }

  async lockMQPeriodically() {
    if (!this.stopped) {
      await this._defaultMQPushConsumer.lockAll();
    }
  }

  async _tryLockLaterAndReconsume(messageQueue, processQueue, delayMills) {
    await sleep(delayMills);
    const lockOK = await this.lockOneMQ(messageQueue);
    if (lockOK) {
      await this._submitConsumeRequestLater(processQueue, messageQueue, 10);
    } else {
      await this._submitConsumeRequestLater(processQueue, messageQueue, 3000);
    }
  }

  async lockOneMQ(messageQueue) {
    if (!this.stopped) {
      const lockOK = await this._defaultMQPushConsumer.lock(messageQueue);
      return lockOK;
    }

    return false;
  }

  async _submitConsumeRequestLater(processQueue, messageQueue, suspendTimeMillis) {
    let timeMillis = suspendTimeMillis;
    if (timeMillis === -1) {
      timeMillis = this._defaultMQPushConsumer.suspendCurrentQueueTimeMillis;
    }

    if (timeMillis < 10) {
      timeMillis = 10;
    } else if (timeMillis > 30000) {
      timeMillis = 30000;
    }

    // 等待指定时间后再重新执行
    await sleep(timeMillis);
    // 根据现有的实现，最外层会不停的循环检查所有的队列调用，所以这里不需要调用
    // this.submitConsumeRequest(null, processQueue, messageQueue);
  }

  /**
   *
   * @param {string} status -
   * @param {any} msg -
   * @param {ProcessQueue} processQueue -
   * @param {MessageQueue} messageQueue -
   * @param {number} suspendCurrentQueueTimeMillis -
   * @return {Promise<boolean>} -
   */
  async _processConsumeResult(status, msg, processQueue, messageQueue, suspendCurrentQueueTimeMillis) {
    let continueConsume = true;
    let commitOffset = -1;

    switch (status) {
      case ConsumeOrderlyStatus.SUCCESS:
        commitOffset = processQueue.commit();
        break;
      case ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT:
        if (await this._checkReconsumeTimes(msg)) {
          processQueue.makeMessageToCosumeAgain(msg);
          await this._submitConsumeRequestLater(
            processQueue,
            messageQueue,
            suspendCurrentQueueTimeMillis);
          continueConsume = false;
        } else {
          commitOffset = processQueue.commit();
        }
        break;
      default:
        break;
    }

    if (commitOffset >= 0 && !processQueue.dropped) {
      this._defaultMQPushConsumer._offsetStore.updateOffset(messageQueue, commitOffset, false);
    }

    return continueConsume;
  }

  _getMaxReconsumeTimes() {
    const maxReconsumeTimes = this._defaultMQPushConsumer.options.maxReconsumeTimes;
    // default reconsume times: Integer.MAX_VALUE
    if (maxReconsumeTimes === -1) {
      return Number.MAX_SAFE_INTEGER;
    }
    return maxReconsumeTimes;
  }

  async _checkReconsumeTimes(msg) {
    let suspend = false;
    if (msg) {
      if (msg.reconsumeTimes >= this._getMaxReconsumeTimes()) {
        msg.properties[MessageConst.PROPERTY_RECONSUME_TIME] = String(msg.reconsumeTimes);
        if (!await this._sendMessageBack(msg)) {
          suspend = true;
          msg.reconsumeTimes++;
        }
      } else {
        suspend = true;
        msg.reconsumeTimes++;
      }
    }
    return suspend;
  }

  async _sendMessageBack(msg) {
    try {
      // max reconsume times exceeded then send to dead letter queue.
      const newMsg = new Message(MixAll.getRetryTopic(this._defaultMQPushConsumer.options.consumerGroup), msg.body);

      const originMsgId = msg.properties[MessageConst.PROPERTY_ORIGIN_MESSAGE_ID];
      newMsg.properties[MessageConst.PROPERTY_ORIGIN_MESSAGE_ID] = !originMsgId ? msg.msgId : originMsgId;
      newMsg.setFlag(msg.getFlag());
      newMsg.properties = msg.properties;
      newMsg.properties[MessageConst.PROPERTY_RETRY_TOPIC] = msg.topic;
      newMsg.properties[MessageConst.PROPERTY_RECONSUME_TIME] = String(msg.reconsumeTimes + 1);
      newMsg.properties[MessageConst.PROPERTY_MAX_RECONSUME_TIMES] = String(this._getMaxReconsumeTimes());
      newMsg.delayTimeLevel = 3 + msg.reconsumeTimes;

      await this._defaultMQPushConsumer._mqClient.getDefaultMQProducer().send(newMsg);
      return true;
    } catch (err) {
      this.logger.error('sendMessageBack exception, group: ' + this.consumerGroup + ' msg: ' + msg.toString(), err);
    }
    return false;
  }
}


module.exports = ConsumeMessageOrderlyService;
