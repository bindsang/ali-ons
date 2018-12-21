'use strict';

const semaphore = require('semaphore');

class RejectedError extends Error {
  // no code
}

class ResourcePoolExecutor {
  /**
   * @param {number} corePoolSize -
   */
  constructor(corePoolSize) {
    this._semaphore = semaphore(corePoolSize);
  }

  get corePoolSize() {
    return this._semaphore.capacity;
  }

  /**
   * @param {any} request -
   * @return {void} -
   */
  submit(request) {
    const sem = this._semaphore;
    if (sem.available(1)) {
      sem.take(async () => {
        try {
          await request.run();
        } finally {
          sem.leave();
        }
      });
    } else {
      throw new RejectedError();
    }
  }
}

module.exports = { ResourcePoolExecutor, RejectedError };
