'use strict';

// add by zhangbing

/**
 * 消费消息的返回结果
 */
module.exports = {
  /**
   * Success consumption
   */
  CONSUME_SUCCESS: 'CONSUME_SUCCESS',
  /**
   * Failure consumption,later try to consume
   */
  RECONSUME_LATER: 'RECONSUME_LATER',
};
