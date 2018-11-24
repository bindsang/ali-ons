'use strict';

/**
 * 消费消息的返回结果
 */
class ConsumeConcurrentlyStatus {
  constructor(value) {
    this.value = value;
  }
}

/**
 * Success consumption
 */
ConsumeConcurrentlyStatus.CONSUME_SUCCESS = new ConsumeConcurrentlyStatus('CONSUME_SUCCESS');
/**
 * Failure consumption,later try to consume
 */
ConsumeConcurrentlyStatus.RECONSUME_LATER = new ConsumeConcurrentlyStatus('RECONSUME_LATER');

/**
 * 顺序消息消费结果
 */
class ConsumeOrderlyStatus {
  constructor(value) {
    this.value = value;
  }
}

/**
 * Success consumption
 */
ConsumeOrderlyStatus.SUCCESS = new ConsumeOrderlyStatus('SUCCESS');
/**
 * Suspend current queue a moment
 */
ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT = new ConsumeOrderlyStatus('SUSPEND_CURRENT_QUEUE_A_MOMENT');

module.exports = { ConsumeConcurrentlyStatus, ConsumeOrderlyStatus };
