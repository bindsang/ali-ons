'use strict';

const semaphore = require('semaphore');

class SemaphoreLock {
  constructor() {
    this._sem = semaphore(1);
  }

  /**
   * @param {function(): Promise<void>} cb -
   * @return {Promise<void>} -
   */
  async synchronized(cb) {
    await new Promise((resolve, reject) => {
      this._sem.take(async () => {
        let e;
        try {
          await cb();
        } catch (err) {
          e = err;
        } finally {
          this._sem.leave();
          if (e) {
            reject(e);
          } else {
            resolve();
          }
        }
      });
    });
  }
}

class MessageQueueLock {
  constructor() {
    /**
     * @type {Map<string, SemaphoreLock>}
     */
    this._mqLockTable = new Map();
  }

  /**
   * @param {MessageQueue} mq -
   * @return {SemaphoreLock} -
   */
  fetchLockObject(mq) {
    let objLock = this._mqLockTable.get(mq);
    if (!objLock) {
      objLock = new SemaphoreLock();
      this._mqLockTable.set(mq.key, objLock);
    }

    return objLock;
  }
}

module.exports = { MessageQueueLock };
