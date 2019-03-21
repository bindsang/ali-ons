'use strict';

const Base = require('sdk-base');
const bsInsert = require('binary-search-insert');
const MessageConst = require('./message/message_const');
const Lock = require('./ext/lock');

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
    this.dropped = false;
    this.lastLockTimestamp = Date.now();
    this.tryUnlockTimes = 0;
    this.msgSize = 0;
    this.lockConsume = new Lock();
  }

  get maxSpan() {
    const msgListCount = this.msgList.length;
    if (msgListCount) {
      return this.msgList[msgListCount - 1].queueOffset - this.msgList[0].queueOffset;
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
      this.msgSize += msg.body.length;
    }
    this.queueOffsetMax = this.msgList[this.msgList.length - 1].queueOffset;
  }

  /**
   * @param {Message} msg -
   * @return {number} -
   */
  remove(msg) {
    let result = -1;
    this.lastConsumeTimestamp = Date.now();
    if (this.msgList.length > 0) {
      result = this.queueOffsetMax + 1;
      const index = this.msgList.findIndex(m => m.queueOffset === msg.queueOffset);
      if (index >= 0) {
        this.msgList.splice(index, 1);
        this.msgSize -= msg.body.length;
      }
      if (this.msgList.length > 0) {
        result = this.msgList[0].queueOffset;
      }
    }
    return result;
  }

  clear() {
    this.msgList = [];
    this.consumingMsgOrderlyList = [];
    this.queueOffsetMax = 0;
    this.msgSize = 0;
  }
  // --------- modify by zhangbing end ------------

  // ------- add by zhangbing start ----------

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
        logger.info(`send expire msg back. topic=${msg.topic}, msgId=${msg.msgId}, storeHost=${msg.storeHost}, queueId=${msg.queueId}, queueOffset=${msg.queueOffset}`);
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
    const len = this.consumingMsgOrderlyList.length;
    if (len > 0) {
      const offset = this.consumingMsgOrderlyList[len - 1].queueOffset;
      for (const msg of this.consumingMsgOrderlyList) {
        this.msgSize -= msg.body.length;
      }
      this.consumingMsgOrderlyList = [];
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

  fillProcessQueueInfo(info) {
    try {
      if (this.msgList.length > 0) {
        info.cachedMsgMinOffset = this.msgList.queueOffset;
        info.cachedMsgMaxOffset = this.msgList[this.msgList.length - 1].queueOffset;
        info.cachedMsgCount = this.msgList.length;
        info.cachedMsgSizeInMiB = Math.floor(this.msgSize / (1024 * 1024));
      }

      if (this.consumingMsgOrderlyList.length > 0) {
        info.transactionMsgMinOffset = this.consumingMsgOrderlyList[0].queueOffset;
        info.transactionMsgMaxOffset = this.consumingMsgOrderlyList[this.consumingMsgOrderlyList.length - 1].queueOffset;
        info.transactionMsgCount = this.consumingMsgOrderlyList.length;
      }

      info.locked = this.locked;
      info.lastLockTimestamp = this.lastLockTimestamp;

      info.droped = this.dropped;
      info.lastPullTimestamp = this.lastPullTimestamp;
      info.lastConsumeTimestamp = this.lastConsumeTimestamp;
    } catch (err) {
      // 什么也不做
    }
  }

  incTryUnlockTimes() {
    this.tryUnlockTimes++;
  }

  hasTempMessage() {
    return this.msgList.length > 0;
  }
  // ------------- add by zhangbing end ----------------
}

ProcessQueue.REBALANCE_LOCK_INTERVAL = 20000;


module.exports = ProcessQueue;
