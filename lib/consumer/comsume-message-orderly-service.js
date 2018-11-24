'use strict';

const { ConsumeOrderlyStatus } = require('./consumer-status');
const MixAll = require('../mix_all');

const MAX_TIME_CONSUME_CONTINUOUSLY = 60000;

module.exports = class ConsumeMessageOrderlyService {
  /**
   * @param {MQPushConsumer} consumer -
   * @param {Function} messageListener -
   */
  constructor(consumer, messageListener) {
    this._defaultMQPushConsumer = consumer;
    this._messageListener = messageListener;
    this.consumerGroup = consumer.consumerGroup;
    this.consumeRequestQueue = [];
  }

  start() {
    if (MessageModel.CLUSTERING === this._defaultMQPushConsumer.messageModel) {
      setTimeout(() => {
        this.lockMQPeriodically()
        setInterval(() => this.lockMQPeriodically(),
        ProcessQueue.REBALANCE_LOCK_INTERVAL)
      }, 1000);
    }
  }

  async shutdown() {
    this.stopped = true;
    this.scheduledExecutorService.shutdown();
    if (MessageModel.CLUSTERING.equals(this._defaultMQPushConsumer.messageModel())) {
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
   * @param {boolean} dispathToConsume -
   */
  async submitConsumeRequest(msg, processQueue, messageQueue, dispathToConsume) {
    if (dispathToConsume) {
      const consumeRequest = new ConsumeRequest(processQueue, messageQueue);
      await consumeRequest.run();
    }
  }

  async lockMQPeriodically() {
    if (!this.stopped) {
      await this._defaultMQPushConsumer.lockAll();
    }
  }

  async tryLockLaterAndReconsume(messageQueue, processQueue, delayMills) {
    setTimeout(async () => {
      const lockOK = await this.lockOneMQ(messageQueue);
      if (lockOK) {
          this._submitConsumeRequestLater(processQueue, messageQueue, 10);
      } else {
          this._submitConsumeRequestLater(processQueue, messageQueue, 3000);
      }
    }, delayMills);
  }

  async lockOneMQ(messageQueue) {
    if (!this.stopped) {
      const lockOK = await this._defaultMQPushConsumer.lock(messageQueue);
      return lockOK;
    }

    return false;
  }

  _submitConsumeRequestLater(processQueue, messageQueue, suspendTimeMillis) {
      let timeMillis = suspendTimeMillis;
      if (timeMillis == -1) {
        timeMillis = this._defaultMQPushConsumer.suspendCurrentQueueTimeMillis;
      }

      if (timeMillis < 10) {
          timeMillis = 10;
      } else if (timeMillis > 30000) {
          timeMillis = 30000;
      }

      setTimeout(() => {
        this.submitConsumeRequest(null, processQueue, messageQueue, true);
      }, timeMillis);
  }

  async _processConsumeResult(msg, status) {
      let continueConsume = true;
      let commitOffset = -1;

      switch (status) {
          case SUCCESS:
              commitOffset = processQueue.commit();
              break;
          case SUSPEND_CURRENT_QUEUE_A_MOMENT:
            if (await this._checkReconsumeTimes(msg)) {
              processQueue.makeMessageToCosumeAgain(msg);
              this._submitConsumeRequestLater(
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
    if (maxReconsumeTimes == -1) {
        return Integer.MAX_VALUE;
    } else {
        return maxReconsumeTimes;
    }
  }

  async _checkReconsumeTimes(msg) {
      let suspend = false;
      if (msg) {
        if (msg.reconsumeTimes >= this._getMaxReconsumeTimes()) {
          msg.properties[MessageConst.PROPERTY_RECONSUME_TIME] = String(msg.reconsumeTimes);
          if (!await this.sendMessageBack(msg)) {
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

  async sendMessageBack(msg) {
    try {
      // max reconsume times exceeded then send to dead letter queue.
      const newMsg = new Message(MixAll.getRetryTopic(this.option.consumerGroup), msg.body);

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
      log.error('sendMessageBack exception, group: ' + this.consumerGroup + ' msg: ' + msg.toString(), err);
    }
    return false;
  }

  class ConsumeRequest {
      private final ProcessQueue processQueue;
      private final MessageQueue messageQueue;

      public ConsumeRequest(processQueue, messageQueue) {
          this.processQueue = processQueue;
          this.messageQueue = messageQueue;
      }

      public ProcessQueue getProcessQueue() {
          return processQueue;
      }

      public MessageQueue getMessageQueue() {
          return messageQueue;
      }

      @Override
      public void run() {
          if (this.processQueue.isDropped()) {
              log.warn('run, the message queue not be able to consume, because it's dropped. {}', this.messageQueue);
              return;
          }

          final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);
          synchronized (objLock) {
              if (MessageModel.BROADCASTING.equals(this._defaultMQPushConsumer.messageModel())
                  || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {
                  final long beginTime = System.currentTimeMillis();
                  for (boolean continueConsume = true; continueConsume; ) {
                      if (this.processQueue.isDropped()) {
                          log.warn('the message queue not be able to consume, because it's dropped. {}', this.messageQueue);
                          break;
                      }

                      if (MessageModel.CLUSTERING.equals(this._defaultMQPushConsumer.messageModel())
                          && !this.processQueue.isLocked()) {
                          log.warn('the message queue not locked, so consume later, {}', this.messageQueue);
                          this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                          break;
                      }

                      if (MessageModel.CLUSTERING.equals(this._defaultMQPushConsumer.messageModel())
                          && this.processQueue.isLockExpired()) {
                          log.warn('the message queue lock expired, so consume later, {}', this.messageQueue);
                          this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                          break;
                      }

                      long interval = System.currentTimeMillis() - beginTime;
                      if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                          this.submitConsumeRequestLater(processQueue, messageQueue, 10);
                          break;
                      }

                      const msg = this.processQueue.takeMessage();
                      if (msg) {
                          final ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);

                          ConsumeOrderlyStatus status = null;

                          ConsumeMessageContext consumeMessageContext = null;
                          if (this._defaultMQPushConsumer.hasHook()) {
                              consumeMessageContext = new ConsumeMessageContext();
                              consumeMessageContext
                                  .setConsumerGroup(this._defaultMQPushConsumer.getConsumerGroup());
                              consumeMessageContext.setMq(messageQueue);
                              consumeMessageContext.setMsgList(msgs);
                              consumeMessageContext.setSuccess(false);
                              // init the consume context type
                              consumeMessageContext.setProps(new HashMap<String, String>());
                              this._defaultMQPushConsumer.executeHookBefore(consumeMessageContext);
                          }

                          long beginTimestamp = System.currentTimeMillis();
                          ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                          boolean hasException = false;
                          try {
                              this.processQueue.getLockConsume().lock();
                              if (this.processQueue.isDropped()) {
                                  log.warn('consumeMessage, the message queue not be able to consume, because it's dropped. {}',
                                      this.messageQueue);
                                  break;
                              }

                              status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
                          } catch (Throwable e) {
                              log.warn('consumeMessage exception: {} Group: {} Msgs: {} MQ: {}',
                                  RemotingHelper.exceptionSimpleDesc(e),
                                  this.consumerGroup,
                                  msgs,
                                  messageQueue);
                              hasException = true;
                          } finally {
                              this.processQueue.getLockConsume().unlock();
                          }

                          if (null == status
                              || ConsumeOrderlyStatus.ROLLBACK == status
                              || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                              log.warn('consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}',
                                  this.consumerGroup,
                                  msgs,
                                  messageQueue);
                          }

                          long consumeRT = System.currentTimeMillis() - beginTimestamp;
                          if (null == status) {
                              if (hasException) {
                                  returnType = ConsumeReturnType.EXCEPTION;
                              } else {
                                  returnType = ConsumeReturnType.RETURNNULL;
                              }
                          } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                              returnType = ConsumeReturnType.TIME_OUT;
                          } else if (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                              returnType = ConsumeReturnType.FAILED;
                          } else if (ConsumeOrderlyStatus.SUCCESS == status) {
                              returnType = ConsumeReturnType.SUCCESS;
                          }

                          if (this._defaultMQPushConsumer.hasHook()) {
                              consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                          }

                          if (null == status) {
                              status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                          }

                          if (this._defaultMQPushConsumer.hasHook()) {
                              consumeMessageContext.setStatus(status.toString());
                              consumeMessageContext
                                  .setSuccess(ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status);
                              this._defaultMQPushConsumer.executeHookAfter(consumeMessageContext);
                          }

                          this.getConsumerStatsManager()
                              .incConsumeRT(this.consumerGroup, messageQueue.getTopic(), consumeRT);

                          continueConsume = this._processConsumeResult(msgs, status, context, this);
                      } else {
                          continueConsume = false;
                      }
                  }
              } else {
                  if (this.processQueue.isDropped()) {
                      log.warn('the message queue not be able to consume, because it's dropped. {}', this.messageQueue);
                      return;
                  }

                  this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
              }
          }
      }

  }
};
