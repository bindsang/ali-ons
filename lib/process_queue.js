'use strict';

const Base = require('sdk-base');
const bsInsert = require('binary-search-insert');
const MessageConst = require('./message/message_const');

const comparator = (a, b) => a.queueOffset - b.queueOffset;

const pullMaxIdleTime = 120000;
const REBALANCE_LOCK_MAX_LIVE_TIME = 30000;

class ProcessQueue extends Base {
  constructor(options = {}) {
    super(options);

    this.msgList = [];
    this.consumingMsgOrderlyList = [];
    this.droped = false;
    this.lastPullTimestamp = Date.now();
    this.lastConsumeTimestamp = Date.now();

    this.locked = false;
    // ------- add by zhangbing begin -------
    this.lastLockTimestamp = Date.now();
    // ------- add by zhangbing end -------
  }

  get maxSpan() {
    const msgCount = this.msgCount;
    if (msgCount) {
      return this.msgList[msgCount - 1].queueOffset - this.msgList[0].queueOffset;
    }
    return 0;
  }

  get msgCount() {
    return this.msgList.length + this.consumingMsgOrderlyList.length;
  }

  get isPullExpired() {
    return Date.now() - this.lastPullTimestamp > pullMaxIdleTime;
  }

  putMessage(msgs) {
    for (const msg of msgs) {
      bsInsert(this.msgList, comparator, msg);
    }
    this.queueOffsetMax = this.msgList[this.msgCount - 1].queueOffset;
  }

  // --------- modify by zhangbing begin ------------
  /**
   * @param {Message} msg -
   * @return {boolean} -
   */
  remove(msg) {
    let result = -1;
    this.lastConsumeTimestamp = Date.now();
    if (this.msgList.length > 0) {
      result = this.queueOffsetMax + 1;
      const index = this.msgList.findIndex(m => m.queueOffset === msg.queueOffset);
      if (index >= 0) {
        this.msgList.splice(index, 1);
      }
      if (this.msgList.length > 0) {
        result = this.msgList.queueOffset;
      }
    }
    return result;
  }

  clear() {
    this.msgList = [];
    this.consumingMsgOrderlyTreeMap.clear();
    this.queueOffsetMax = 0;
  }
  // --------- modify by zhangbing end ------------

  get lockExpired() {
    return (Date.now() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
  }

  /**
   * @param {MQPushConsumer} pushConsumer -
   */
  async cleanExpiredMsg(pushConsumer) {
    if (pushConsumer.consumeOrderly) {
      return;
    }

    const logger = pushConsumer.options.logger;
    const loop = this.msgList.length < 16 ? this.msgList.length : 16;
    for (let i = 0; i < loop; i++) {
      let msg = null;
      if (this.msgList.length > 0 &&
        (Date.now() - Number(this.msgList[0].properties[MessageConst.CONSUME_START_TIME])) > pushConsumer.consumeTimeout * 60 * 1000) {
        msg = this.msgList[0];
      } else {
        break;
      }
      try {
        await pushConsumer.sendMessageBack(msg, 3);
        logger.info('send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}', msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
        if (this.msgList.length > 0 && msg.queueOffset === this.msgList[0].queueOffset) {
          try {
            this.remove(msg);
          } catch (err) {
            logger.error('send expired msg exception', err);
          }
        }
      } catch (err) {
        logger.error('send expired msg exception', err);
      }
    }
  }

  /**
   * @return {Message} -
   */
  takeMessage() {
    const now = Date.now();
    this.lastConsumeTimestamp = now;
    if (this.msgList.length > 0) {
      const msg = this.msgList.shift();
      this.consumingMsgOrderlyList.push(msg);
      return msg;
    }

    return null;
  }

  commit() {
    const offset = this.consumingMsgOrderlyList[this.consumingMsgOrderlyList.length - 1].queueOffset;
    this.consumingMsgOrderlyList.clear();
    if (Number.isInteger(offset)) {
      return offset + 1;
    }
    return -1;
  }

  makeMessageToCosumeAgain(msg) {
    const index = this.consumingMsgOrderlyList.indexOf(msg);
    this.consumingMsgOrderlyList.splice(index, 1);

    // 按顺序插入到this.msgList队列中
    bsInsert(this.msgList, comparator, msg);
  }
}

ProcessQueue.REBALANCE_LOCK_INTERVAL = 20000;


module.exports = ProcessQueue;
