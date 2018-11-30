'use strict';

const LOCK_FIELD = Symbol('locked');

// 模拟资源锁
module.exports = class Lock {
  constructor() {
    this[LOCK_FIELD] = false;
    /**
     * @type {Map<object, () => void>}
     */
    this._map = new Map();
  }

  /**
   * @return {void} -
   */
  async lock() {
    await doLock(this, -1);
  }

  /**
   * @param {number} timeout -
   * @return {Promise<boolean>} -
   */
  async tryLock(timeout) {
    const success = await doLock(this, timeout);
    return success;
  }

  unlock() {
    const locked = this[LOCK_FIELD];
    if (!locked) {
      throw new Error('not locked');
    }

    this[LOCK_FIELD] = false;
    if (this._map.size > 0) {
      // 找到第一个获取锁的回调函数
      const firstFn = this._map.entries().next().value[1];
      firstFn();
    }
  }
};

async function doLock(lock, timeout) {
  const locked = lock[LOCK_FIELD];
  if (locked) {
    const obj = {};
    const success = await new Promise(resolve => {
      lock._map.set(obj, () => resolve(true));
      if (timeout > 0) {
        setTimeout(() => {
          // 超时
          resolve(false);
        }, timeout);
      }
    });

    // 从等待锁的对象中中移除当前函数
    lock._map.delete(obj);

    if (!success) {
      // 未获取到锁
      return false;
    }
  }

  // 没有别的地方锁住，当前调用方法获得锁
  lock[LOCK_FIELD] = true;
  return true;
}
