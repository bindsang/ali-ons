'use strict';

const assert = require('assert');
const is = require('is-type-of');
const utils = require('../utils');
const logger = require('../logger');
const MixAll = require('../mix_all');
const MQClient = require('../mq_client');
const MQProducer = require('../producer/mq_producer');
const sleep = require('mz-modules/sleep');
const ClientConfig = require('../client_config');
const ProcessQueue = require('../process_queue');
const PullStatus = require('../consumer/pull_status');
const PullSysFlag = require('../utils/pull_sys_flag');
const ConsumeFromWhere = require('./consume_from_where');
const MessageModel = require('../protocol/message_model');
const ConsumeType = require('../protocol/consume_type');
const ReadOffsetType = require('../store/read_offset_type');
const LocalFileOffsetStore = require('../store/local_file');
const LocalMemoryOffsetStore = require('../store/local_memory');
const RemoteBrokerOffsetStore = require('../store/remote_broker');
const AllocateMessageQueueAveragely = require('./rebalance/allocate_message_queue_averagely');
const Message = require('../message/message');
const MessageConst = require('../message/message_const');
const ConsumeMessageConcurrentlyService = require('./consume-message-concurrently-service');
const ConsumeMessageOrderlyService = require('./consume-message-orderly-service');

const defaultOptions = {
  logger,
  persistent: false, // 是否持久化消费进度
  isBroadcast: false, // 是否是广播模式（默认集群消费模式）
  brokerSuspendMaxTimeMillis: 1000 * 15, // 长轮询模式，Consumer连接在Broker挂起最长时间
  pullTimeDelayMillsWhenException: 3000, // 拉消息异常时，延迟一段时间再拉
  pullTimeDelayMillsWhenFlowControl: 5000, // 进入流控逻辑，延迟一段时间再拉
  consumerTimeoutMillisWhenSuspend: 1000 * 30, // 长轮询模式，Consumer超时时间（必须要大于brokerSuspendMaxTimeMillis）
  consumerGroup: MixAll.DEFAULT_CONSUMER_GROUP,
  consumeFromWhere: ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, // Consumer第一次启动时，从哪里开始消费
  /**
   * Consumer第一次启动时，如果回溯消费，默认回溯到哪个时间点，数据格式如下，时间精度秒：
   * 20131223171201
   * 表示2013年12月23日17点12分01秒
   * 默认回溯到相对启动时间的半小时前
   */
  consumeTimestamp: utils.timeMillisToHumanString(Date.now() - 1000 * 60 * 30),
  pullThresholdForQueue: 500, // 本地队列消息数超过此阀值，开始流控
  pullThresholdSizeForQueue: 50,
  pullInterval: 0, // 拉取消息的频率, 如果为了降低拉取速度，可以设置大于0的值
  consumeMessageBatchMaxSize: 1, // 消费一批消息，最大数
  pullBatchSize: 32, // 拉消息，一次拉多少条
  parallelConsumeLimit: 1, // 并发消费消息限制
  postSubscriptionWhenPull: true, // 是否每次拉消息时，都上传订阅关系
  consumeConcurrentlyMaxSpan: 2000, // 全局有序消息中本地最多拉取暂存的消息数量
  pullTimeDelayMillsWhenSuspend: 1000, // 位于suspend状态时，延迟拉取消息的时间
  unlockDelayTimeMills: 20000,
  pullThresholdSizeForTopic: -1,
  pullThresholdForTopic: -1,
  allocateMessageQueueStrategy: new AllocateMessageQueueAveragely(), // 队列分配算法，应用可重写
  maxReconsumeTimes: 16, // 最大重试次数
};

class MQPushConsumer extends ClientConfig {
  constructor(options) {
    assert(options && options.consumerGroup, '[MQPushConsumer] options.consumerGroup is required');
    const mergedOptions = Object.assign({ initMethod: 'init' }, defaultOptions, options);
    assert(mergedOptions.parallelConsumeLimit <= mergedOptions.pullBatchSize,
      '[MQPushConsumer] options.parallelConsumeLimit must lte pullBatchSize');
    super(mergedOptions);

    // @example:
    // pullFromWhichNodeTable => {
    //   '[topic="TEST_TOPIC", brokerName="qdinternet-03", queueId="1"]': 0
    // }
    this._pullFromWhichNodeTable = new Map();
    this._subscriptions = new Map();
    this._handles = new Map();
    this._topicSubscribeInfoTable = new Map();
    this._processQueueTable = new Map();
    this._inited = false;
    this._isClosed = false;

    if (this.messageModel === MessageModel.CLUSTERING) {
      this.changeInstanceNameToPID();
    }

    this._mqClient = MQClient.getAndCreateMQClient(this);
    this._offsetStore = this.newOffsetStoreInstance();

    this._consumeOrderly = false;
    this._consumeMessageService = null;
    this._consumeTimeout = 15;
    this._suspendCurrentQueueTimeMillis = 1000;
    this._consumerStartTimestamp = Date.now();
    this._pause = false;
    this._queueFlowControlTimes = 0;
    this._queueMaxSpanFlowControlTimes = 0;

    this._mqClient.on('error', err => this._handleError(err));
    this._offsetStore.on('error', err => this._handleError(err));
  }

  get isPause() {
    return this._pause;
  }

  get consumeTimeout() {
    return this._consumeTimeout;
  }

  get consumeOrderly() {
    return this._consumeOrderly;
  }

  get suspendCurrentQueueTimeMillis() {
    return this._suspendCurrentQueueTimeMillis;
  }

  get logger() {
    return this.options.logger;
  }

  get subscriptions() {
    return this._subscriptions;
  }

  get processQueueTable() {
    return this._processQueueTable;
  }

  get parallelConsumeLimit() {
    return this.options.parallelConsumeLimit;
  }

  get consumerGroup() {
    if (this.namespace) {
      return `${this.namespace}%${this.options.consumerGroup}`;
    }
    return this.options.consumerGroup;
  }

  get messageModel() {
    return this.options.isBroadcast ? MessageModel.BROADCASTING : MessageModel.CLUSTERING;
  }

  get consumeType() {
    return ConsumeType.CONSUME_PASSIVELY;
  }

  get consumeFromWhere() {
    return this.options.consumeFromWhere;
  }

  get allocateMessageQueueStrategy() {
    return this.options.allocateMessageQueueStrategy;
  }

  async init() {
    this._mqClient.registerConsumer(this.consumerGroup, this);
    await MQProducer.getDefaultProducer(this.options);
    await this._mqClient.ready();
    await this._offsetStore.load();
    this.logger.info('[mq:consumer] consumer started');
    this._inited = true;

    // 订阅重试 TOPIC
    if (this.messageModel === MessageModel.CLUSTERING) {
      const retryTopic = MixAll.getRetryTopic(this.consumerGroup);
      const retrySubscriptionData = this.buildSubscriptionData(this.consumerGroup, retryTopic);
      this.subscriptions.set(retryTopic, {
        handler: null, // 该属性没有用到，这里暂时设置为空
        subscriptionData: retrySubscriptionData,
      });

      this._consumeTopic(retryTopic, null);
    }
  }

  /**
   * close the consumer
   */
  async close() {
    this._isClosed = true;
    // ------- add by zhangbing begin -------
    await this._consumeMessageService.close();
    // ------- add by zhangbing end -------
    await this.persistConsumerOffset();
    this._pullFromWhichNodeTable.clear();
    this._subscriptions.clear();
    this._topicSubscribeInfoTable.clear();
    this._processQueueTable.clear();

    await this._mqClient.unregisterConsumer(this.consumerGroup);
    await this._mqClient.close();
    this.logger.info('[mq:consumer] consumer closed');
    this.emit('close');
  }

  newOffsetStoreInstance() {
    if (this.messageModel === MessageModel.BROADCASTING) {
      if (this.options.persistent) {
        return new LocalFileOffsetStore(this._mqClient, this.consumerGroup);
      }
      return new LocalMemoryOffsetStore(this._mqClient, this.consumerGroup);
    }
    return new RemoteBrokerOffsetStore(this._mqClient, this.consumerGroup);
  }

  /**
   * subscribe
   * @param {String} topic - topic
   * @param {String} subExpression - tag
   * @param {Function} handler - message handler
   * @param {boolean} isOrder - isOrder
   * @return {void}
   */
  subscribe(topic, subExpression, handler, isOrder) {
    // 添加 namespace 前缀
    topic = this.formatTopic(topic);

    if (arguments.length === 2) {
      handler = subExpression;
      subExpression = null;
    }
    assert(is.asyncFunction(handler), '[MQPushConsumer] handler should be a asyncFunction');
    assert(!this.subscriptions.has(topic), `[MQPushConsumer] ONLY one handler allowed for topic=${topic}`);

    const subscriptionData = this.buildSubscriptionData(this.consumerGroup, topic, subExpression);
    this.subscriptions.set(topic, {
      handler,
      subscriptionData,
    });

    this._consumeOrderly = isOrder;
    this._consumeMessageService = isOrder
      ? new ConsumeMessageOrderlyService(this, handler)
      : new ConsumeMessageConcurrentlyService(this, handler);

    this._consumeMessageService.init();
    this._consumeTopic(topic, subExpression);
  }
  /**
   * @param {string} topic -
   * @param {string} subExpression -
   */
  async _consumeTopic(topic, subExpression) {
    try {
      await this.ready();
      // 如果 topic 没有路由信息，先更新一下
      if (!this._topicSubscribeInfoTable.has(topic)) {
        await this._mqClient.updateAllTopicRouterInfo();
        await this._mqClient.sendHeartbeatToAllBroker();
        await this._mqClient.doRebalance();
      }

      // 消息消费循环
      while (!this._isClosed && this.subscriptions.has(topic)) {
        await this._consumeMessageLoop(topic);
      }
      this.logger.info('[MQPushConsumer] cancel subscribe for topic=%s, subExpression=%s', topic, subExpression);
    } catch (err) {
      this._handleError(err);
    }
  }

  async _consumeMessageLoop(topic) {
    const mqList = this._topicSubscribeInfoTable.get(topic);
    let hasMsg = false;
    if (mqList && mqList.length) {
      for (const mq of mqList) {
        const item = this._processQueueTable.get(mq.key);
        if (item) {
          const pq = item.processQueue;
          this.logger.debug('[MQPushConsumer] process msg for processQueue=%s, msgCount=%', mq.key, pq.msgList.length);
          while (pq.msgList.length) {
            hasMsg = true;
            let msgs;
            if (this.parallelConsumeLimit > pq.msgCount) {
              msgs = pq.msgList.slice(0, pq.msgCount);
            } else {
              msgs = pq.msgList.slice(0, this.parallelConsumeLimit);
            }
            // 并发消费任务
            const consumeTasks = [];
            for (const msg of msgs) {
              consumeTasks.push(this._consumeMessageService.submitConsumeRequest(msg, pq, mq));
            }
            // 必须全部成功
            try {
              await Promise.all(consumeTasks);
            } catch (err) {
              continue;
            }
          }
        }
      }
    }

    if (!hasMsg) {
      const changedEvent = `topic_${topic}_changed`;
      this.logger.debug(`[MQPushConsumer] waiting event for ${changedEvent}`);
      await this.await(changedEvent);
    }
  }

  // remove by zhangbing
  // async consumeSingleMsg(handler, msg, mq, pq) {
  //   // 集群消费模式下，如果消费失败，反复重试
  //   while (!this._isClosed) {
  //     if (msg.reconsumeTimes > this.options.maxReconsumeTimes) {
  //       this.logger.warn('[MQPushConsumer] consume message failed, drop it for reconsumeTimes=%d and maxReconsumeTimes=%d, msgId: %s, originMsgId: %s',
  //         msg.reconsumeTimes, this.options.maxReconsumeTimes, msg.msgId, msg.originMessageId);
  //       return;
  //     }

  //     utils.resetRetryTopic(msg, this.consumerGroup);

  //     try {
  //       const value = await handler(msg, mq, pq);
  //       if (value !== MQPushConsumer.ACTION_RETRY) {
  //         return;
  //       }
  //     } catch (err) {
  //       err.message = `process mq message failed, topic: ${msg.topic}, msgId: ${msg.msgId}, ${err.message}`;
  //       this.emit('error', err);
  //     }

  //     if (this.messageModel === MessageModel.CLUSTERING) {
  //       // 发送重试消息
  //       try {
  //         // delayLevel 为 0 代表由服务端控制重试间隔
  //         await this.sendMessageBack(msg, 0, mq.brokerName, this.consumerGroup);
  //         return;
  //       } catch (err) {
  //         this.emit('error', err);
  //         this.logger.error(
  //           '[MQPushConsumer] send reconsume message failed, fallback to local retry, msgId: %s',
  //           msg.msgId
  //         );
  //         // 重试消息发送失败，本地重试
  //         await this._sleep(5000);
  //       }
  //       // 本地重试情况下需要给 reconsumeTimes +1
  //       msg.reconsumeTimes++;
  //     } else {
  //       this.logger.warn('[MQPushConsumer] BROADCASTING consume message failed, drop it, msgId: %s', msg.msgId);
  //       return;
  //     }
  //   }
  //   this.logger.info('[MQPushConsumer] consumer is closed, skip consume msg, msgId=%s', msg.msgId);
  // }
  // remove end

  /**
   * construct subscription data
   * @param {String} consumerGroup - consumer group name
   * @param {String} topic - topic
   * @param {String} subString - tag
   * @return {Object} subscription
   */
  buildSubscriptionData(consumerGroup, topic, subString) {
    const subscriptionData = {
      topic,
      subString,
      classFilterMode: false,
      tagsSet: [],
      codeSet: [],
      subVersion: Date.now(),
      expressionType: 'TAG',
    };
    if (is.nullOrUndefined(subString) || subString === '*' || subString === '') {
      subscriptionData.subString = '*';
    } else {
      const tags = subString.split('||');
      for (let tag of tags) {
        tag = tag.trim();
        if (tag) {
          subscriptionData.tagsSet.push(tag);
          subscriptionData.codeSet.push(utils.hashCode(tag));
        }
      }
    }
    return subscriptionData;
  }

  async persistConsumerOffset() {
    const mqs = [];
    for (const key of this._processQueueTable.keys()) {
      if (this._processQueueTable.get(key)) {
        mqs.push(this._processQueueTable.get(key).messageQueue);
      }
    }
    await this._offsetStore.persistAll(mqs);
  }

  /**
   * pull message from queue
   * @param {MessageQueue} messageQueue - message queue
   * @return {void}
   */
  pullMessageQueue(messageQueue) {
    const _self = this;
    (async () => {
      while (!_self._isClosed && _self._processQueueTable.has(messageQueue.key)) {
        try {
          await _self.executePullRequestImmediately(messageQueue);
          await _self._sleep(_self.options.pullInterval);
        } catch (err) {
          if (!_self._isClosed) {
            err.name = 'MQConsumerPullMessageError';
            err.message = `[mq:consumer] pull message for queue: ${messageQueue.key}, occurred error: ${err.message}`;
            _self._handleError(err);
            await _self._sleep(_self.options.pullTimeDelayMillsWhenException);
          }
        }
      }
    })();
  }

  /**
   * execute pull message immediately
   * @param {MessageQueue} messageQueue - messageQueue
   * @return {void}
   */
  async executePullRequestImmediately(messageQueue) {
    // close or queue removed
    if (!this._processQueueTable.has(messageQueue.key)) {
      return;
    }
    const pullRequest = this._processQueueTable.get(messageQueue.key);
    const processQueue = pullRequest.processQueue;
    // queue dropped
    if (processQueue.dropped) {
      return;
    }

    // ---------- add by zhangbing begin ------------

    if (this.isPause) {
      this.logger.warn(`consumer was paused, execute pull request later. instanceName=${this.instanceName}, group=${this.consumerGroup}`);
      await this._sleep(this.options.pullTimeDelayMillsWhenSuspend);
      return;
    }

    // ---------- add by zhangbing end ------------

    // flow control
    const size = processQueue.msgCount;
    if (size > this.options.pullThresholdForQueue) {
      if ((this._queueFlowControlTimes++ % 1000) === 0) {
        this.logger.warn(
          `the cached message count exceeds the threshold ${this.options.pullThresholdForQueue}, ` +
          `so do flow control, minOffset=${processQueue.msgList[0].queueOffset}, ` +
          `maxOffset=${processQueue.msgList[processQueue.msgList.length - 1].queueOffset}, count=${size}, ` +
          `pullRequest=${pullRequest}, flowControlTimes=${this._queueFlowControlTimes}`);
      }
      await this._sleep(this.options.pullTimeDelayMillsWhenFlowControl);
      return;
    }

    // ---------- add by zhangbing begin ------------

    const cachedMessageSizeInMiB = Math.floor(processQueue.msgSize / (1024 * 1024));
    if (cachedMessageSizeInMiB > this.options.pullThresholdSizeForQueue) {
      if ((this._queueFlowControlTimes++ % 1000) === 0) {
        this.logger.warn(
          `the cached message size exceeds the threshold ${this.options.pullThresholdSizeForQueue} MiB, ` +
          `so do flow control, minOffset=${processQueue.msgList[0].queueOffset}, ` +
          `maxOffset=${processQueue.msgList[processQueue.msgList.length - 1].queueOffset}, count=${size}, ` +
          `size=${cachedMessageSizeInMiB} MiB, pullRequest=${pullRequest}, flowControlTimes=${this._flowControlTimes}`);
      }
      await this._sleep(pullRequest, this.options.pullTimeDelayMillsWhenFlowControl);
      return;
    }

    if (!this.consumeOrderly) {
      if (processQueue.maxSpan > this.options.consumeConcurrentlyMaxSpan) {
        await this._sleep(this.options.pullTimeDelayMillsWhenFlowControl);
        if ((this._queueMaxSpanFlowControlTimes++ % 1000) === 0) {
          this.logger.warn(
            "the queue's messages, span too long, so do flow control, " +
            `minOffset=${processQueue.msgList[0].queueOffset}, ` +
            `maxOffset=${processQueue.msgList[processQueue.msgList.length - 1].queueOffset}, ` +
            `maxSpan=${processQueue.maxSpan}, pullRequest=${pullRequest}, ` +
            `flowControlTimes=${this._queueMaxSpanFlowControlTimes}`);
        }
        return;
      }
    } else {
      if (processQueue.locked) {
        if (!pullRequest.lockedFirst) {
          const offset = await this.computePullFromWhere(pullRequest.messageQueue);
          const brokerBusy = offset < pullRequest.nextOffset;
          this.logger.info(`the first time to pull message, so fix offset from broker. pullRequest: ${pullRequest} NewOffset: ${offset} brokerBusy: ${brokerBusy}`);
          if (brokerBusy) {
            this.logger.info(`[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: ${pullRequest} NewOffset: ${offset}`);
          }

          pullRequest.lockedFirst = true;
          pullRequest.nextOffset = offset;
        }
      } else {
        await this._sleep(this.options.pullTimeDelayMillsWhenException);
        this.logger.info(`pull message later because not locked in broker, ${pullRequest}`);
        return;
      }
    }
    // ---------- add by zhangbing end ------------

    processQueue.lastPullTimestamp = Date.now();
    const data = this.subscriptions.get(messageQueue.topic);
    const subscriptionData = data && data.subscriptionData;
    if (!subscriptionData) {
      this.logger.warn('[mq:consumer] execute pull request, but subscriptionData not found, topic: %s, queueId: %s', messageQueue.topic, messageQueue.queueId);
      await this._sleep(this.options.pullTimeDelayMillsWhenException);
      return;
    }

    let commitOffset = 0;
    const subExpression = this.options.postSubscriptionWhenPull ? subscriptionData.subString : null;
    const subVersion = subscriptionData.subVersion;

    // cluster model
    if (MessageModel.CLUSTERING === this.messageModel) {
      const offset = await this._offsetStore.readOffset(pullRequest.messageQueue, ReadOffsetType.READ_FROM_MEMORY);
      if (offset) {
        commitOffset = offset;
      }
    }

    this.logger.info('[MQPushConsumer] start to pull message from queue: %s, nextOffset: %s, commitOffset: %s, subExpression: %s, subVersion: %s',
      messageQueue.key, pullRequest.nextOffset, commitOffset, subExpression, subVersion);
    const pullResult = await this.pullKernelImpl(messageQueue, subExpression, subVersion, pullRequest.nextOffset, commitOffset);
    this.updatePullFromWhichNode(messageQueue, pullResult.suggestWhichBrokerId);
    const originOffset = pullRequest.nextOffset;
    // update next pull offset
    pullRequest.nextOffset = pullResult.nextBeginOffset;

    this.logger.info('[MQPushConsumer] pull message result: %s from queue: %s, requestOffset: %s, nextBeginOffset: %s, minOffset: %s, maxOffset: %s, suggestWhichBrokerId: %s',
      pullResult.pullStatus, messageQueue.key, originOffset, pullResult.nextBeginOffset, pullResult.minOffset, pullResult.maxOffset, pullResult.suggestWhichBrokerId);

    switch (pullResult.pullStatus) {
      case PullStatus.FOUND:
      {
        const pullRT = Date.now() - processQueue.lastPullTimestamp;
        const msgIds = pullResult.msgFoundList.map(v => v.msgId);
        this.logger.info('[MQPushConsumer] pull message success, found new message size: %d, topic: %s, msgId: [%s], consumerGroup: %s, messageQueue: %s, cost: %dms.',
          pullResult.msgFoundList.length, messageQueue.topic, msgIds.join(','), this.consumerGroup, messageQueue.key, pullRT);

        // submit to consumer
        processQueue.putMessage(pullResult.msgFoundList);
        this.emit(`topic_${messageQueue.topic}_changed`);
        break;
      }
      case PullStatus.NO_NEW_MSG:
      case PullStatus.NO_MATCHED_MSG:
        this.logger.debug('[mq:consumer] no new message for topic: %s at message queue => %s', subscriptionData.topic, messageQueue.key);
        this.correctTagsOffset(pullRequest);
        break;
      case PullStatus.OFFSET_ILLEGAL:
        this.logger.warn('[mq:consumer] the pull request offset illegal, message queue => %s, the originOffset => %d, pullResult => %j', messageQueue.key, originOffset, pullResult);
        this._offsetStore.updateOffset(messageQueue, pullRequest.nextOffset);
        break;
      default:
        break;
    }
  }

  async pullKernelImpl(messageQueue, subExpression, subVersion, offset, commitOffset) {
    let sysFlag = PullSysFlag.buildSysFlag( //
      commitOffset > 0, // commitOffset
      true, // suspend
      !!subExpression, // subscription
      false // class filter
    );
    const result = await this.findBrokerAddress(messageQueue);
    if (!result) {
      throw new Error(`The broker[${messageQueue.brokerName}] not exist`);
    }

    // Slave不允许实时提交消费进度，可以定时提交
    if (result.slave) {
      sysFlag = PullSysFlag.clearCommitOffsetFlag(sysFlag);
    }

    const requestHeader = {
      consumerGroup: this.consumerGroup,
      topic: messageQueue.topic,
      queueId: messageQueue.queueId,
      queueOffset: offset,
      maxMsgNums: this.options.pullBatchSize,
      sysFlag,
      commitOffset,
      suspendTimeoutMillis: this.options.brokerSuspendMaxTimeMillis,
      subscription: subExpression,
      subVersion,
    };
    const pullResult = await this._mqClient.pullMessage(result.brokerAddr, requestHeader, this.options.consumerTimeoutMillisWhenSuspend);
    const subscription = this.subscriptions.get(messageQueue.topic);
    const subscriptionData = subscription.subscriptionData;
    if (subscriptionData.tagsSet.length > 0 && !subscriptionData.classFilterMode) {
      pullResult.msgFoundList = pullResult.msgFoundList.filter(msg => {
        const matched = !msg.tags || subscriptionData.tagsSet.includes(msg.tags);
        if (!matched) {
          this.logger.debug('[MQPushConsumer] message filter by tags=, msg.tags=%s', subExpression, msg.tags);
        }
        return matched;
      });
    }

    return pullResult;
  }

  async findBrokerAddress(messageQueue) {
    let findBrokerResult = this._mqClient.findBrokerAddressInSubscribe(
      messageQueue.brokerName, this.recalculatePullFromWhichNode(messageQueue), false);

    if (!findBrokerResult) {
      await this._mqClient.updateTopicRouteInfoFromNameServer(messageQueue.topic);
      findBrokerResult = this._mqClient.findBrokerAddressInSubscribe(
        messageQueue.brokerName, this.recalculatePullFromWhichNode(messageQueue), false);
    }
    return findBrokerResult;
  }

  recalculatePullFromWhichNode(messageQueue) {
    // @example:
    // pullFromWhichNodeTable => {
    //   '[topic="TEST_TOPIC", brokerName="qdinternet-03", queueId="1"]': 0
    // }
    return this._pullFromWhichNodeTable.get(messageQueue.key) || MixAll.MASTER_ID;
  }

  correctTagsOffset(pullRequest) {
    // 仅当已拉下的消息消费完的情况下才更新 offset
    if (pullRequest.processQueue.msgCount === 0) {
      this._offsetStore.updateOffset(pullRequest.messageQueue, pullRequest.nextOffset, true);
    }
  }

  updatePullFromWhichNode(messageQueue, brokerId) {
    this._pullFromWhichNodeTable.set(messageQueue.key, brokerId);
  }

  /**
   * update subscription data
   * @param {String} topic - topic
   * @param {Array} info - info
   * @return {void}
   */
  updateTopicSubscribeInfo(topic, info) {
    if (this._subscriptions.has(topic)) {
      this._topicSubscribeInfoTable.set(topic, info);
    }
  }

  /**
   * whether need update
   * @param {String} topic - topic
   * @return {Boolean} need update?
   */
  isSubscribeTopicNeedUpdate(topic) {
    if (this._subscriptions && this._subscriptions.has(topic)) {
      return !this._topicSubscribeInfoTable.has(topic);
    }
    return false;
  }

  /**
   * rebalance
   * @return {void}
   */
  async doRebalance() {
    if (this._pause) {
      return;
    }

    for (const topic of this.subscriptions.keys()) {
      await this.rebalanceByTopic(topic);
    }

    this.truncateMessageQueueNotMyTopic();
  }

  async rebalanceByTopic(topic) {
    this.logger.info('[mq:consumer] rebalanceByTopic: %s, messageModel: %s', topic, this.messageModel);
    const mqSet = this._topicSubscribeInfoTable.get(topic); // messageQueue list
    if (!mqSet || !mqSet.length) {
      this.logger.warn('[mq:consumer] doRebalance, %s, but the topic[%s] not exist.', this.consumerGroup, topic);
      return;
    }

    let changed;
    let allocateResult = mqSet;
    if (this.options.isBroadcast) {
      changed = await this.updateProcessQueueTableInRebalance(topic, mqSet);
      await this.messageQueueChanged(topic, mqSet, allocateResult);
    } else {
      const cidAll = await this._mqClient.findConsumerIdList(topic, this.consumerGroup);
      this.logger.info('[mq:consumer] rebalance topic: %s, with consumer ids: %j', topic, cidAll);
      if (cidAll && cidAll.length) {
        // 排序
        mqSet.sort(compare);
        cidAll.sort();

        allocateResult = this.allocateMessageQueueStrategy.allocate(this.consumerGroup, this._mqClient.clientId, mqSet, cidAll);
        this.logger.info('[mq:consumer] allocate queue for group: %s, clientId: %s, result: %j', this.consumerGroup, this._mqClient.clientId, allocateResult);
        changed = await this.updateProcessQueueTableInRebalance(topic, allocateResult);
      }
    }
    if (changed) {
      this.logger.info('[mq:consumer] do rebalance and message queue changed, topic: %s, mqSet: %j', topic, allocateResult);
      await this.messageQueueChanged(topic, mqSet, allocateResult);
      this.emit(`topic_${topic}_queue_changed`);
    }
  }

  truncateMessageQueueNotMyTopic() {
    const keys = [ ...this._processQueueTable.keys() ];
    for (const key of keys) {
      const { messageQueue, processQueue } = this._processQueueTable.get(key);
      if (!this.subscriptions.has(messageQueue.topic)) {
        this._processQueueTable.delete(key);
        if (processQueue) {
          processQueue.dropped = true;
          this.logger.info('[mq:consumer] doRebalance, %s, truncateMessageQueueNotMyTopic remove unnecessary mq, %s', this.consumerGroup, key);
        }
      }
    }
  }

  /**
   * update process queue
   * @param {String} topic - topic
   * @param {Array} mqSet - message queue set
   * @return {void}
   */
  async updateProcessQueueTableInRebalance(topic, mqSet) {
    let changed = false;
    // delete unnecessary queue
    for (const key of this._processQueueTable.keys()) {
      const obj = this._processQueueTable.get(key);
      const messageQueue = obj.messageQueue;
      const processQueue = obj.processQueue;

      if (topic === messageQueue.topic) {
        // not found in mqSet, that means the process queue is unnecessary.
        if (!mqSet.some(mq => mq.key === messageQueue.key)) {
          processQueue.dropped = true;
          await this.removeProcessQueue(messageQueue);
          changed = true;
        } else if (processQueue.isPullExpired && this.consumeType === ConsumeType.CONSUME_PASSIVELY) {
          processQueue.dropped = true;
          await this.removeProcessQueue(messageQueue);
          changed = true;
          this.logger.warn('[MQPushConsumer] BUG doRebalance, %s, remove unnecessary mq=%s, because pull is pause, so try to fixed it',
            this.consumerGroup, messageQueue.key);
        }
      }
    }

    for (const messageQueue of mqSet) {
      if (this._processQueueTable.has(messageQueue.key)) {
        continue;
      }

      if (this._consumeOrderly && !await this.lock(messageQueue)) {
        this.logger.warn(`doRebalance, ${this.consumerGroup}, add a new mq failed, ${messageQueue.key}, because lock failed`);
        continue;
      }

      this.removeDirtyOffset(messageQueue);

      const nextOffset = await this.computePullFromWhere(messageQueue);
      // double check
      if (this._processQueueTable.has(messageQueue.key)) {
        continue;
      }

      if (nextOffset >= 0) {
        const processQueue = new ProcessQueue();
        changed = true;
        this._processQueueTable.set(messageQueue.key, {
          messageQueue,
          processQueue,
          nextOffset,
        });
        // start to pull this queue;
        this.pullMessageQueue(messageQueue);

        this.logger.info('[mq:consumer] doRebalance, %s, add a new messageQueue, %j, its nextOffset: %s', this.consumerGroup, messageQueue, nextOffset);
      } else {
        this.logger.warn('[mq:consumer] doRebalance, %s, new messageQueue, %j, has invalid nextOffset: %s', this.consumerGroup, messageQueue, nextOffset);
      }
    }

    // ---------- add by zhangbing begin ---------
    await this._mqClient.sendHeartbeatToAllBroker();
    // -----------add by zhangbing end -----------
    return changed;
  }

  /**
   * compute consume offset
   * @param {MessageQueue} messageQueue - message queue
   * @return {Promise<number>} offset
   */
  async computePullFromWhere(messageQueue) {
    try {
      const lastOffset = await this._offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE);
      this.logger.info('[mq:consumer] read lastOffset => %s from store, topic="%s", brokerName="%s", queueId="%s"', lastOffset, messageQueue.topic, messageQueue.brokerName, messageQueue.queueId);

      let result = -1;
      switch (this.consumeFromWhere) {
        case ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
        case ConsumeFromWhere.CONSUME_FROM_MIN_OFFSET:
        case ConsumeFromWhere.CONSUME_FROM_MAX_OFFSET:
        case ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET:
          // 第二次启动，根据上次的消费位点开始消费
          if (lastOffset >= 0) {
            result = lastOffset;
          } else if (lastOffset === -1) { // 第一次启动，没有记录消费位点
            // 重试队列则从队列头部开始
            if (messageQueue.topic.indexOf(MixAll.RETRY_GROUP_TOPIC_PREFIX) === 0) {
              result = 0;
            } else { // 正常队列则从队列尾部开始
              return await this._mqClient.maxOffset(messageQueue);
            }
          }
          break;
        case ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET:
          // 第二次启动，根据上次的消费位点开始消费
          if (lastOffset >= 0) {
            result = lastOffset;
          } else {
            result = 0;
          }
          break;
        case ConsumeFromWhere.CONSUME_FROM_TIMESTAMP:
          // 第二次启动，根据上次的消费位点开始消费
          if (lastOffset >= 0) {
            result = lastOffset;
          } else if (lastOffset === -1) { // 第一次启动，没有记录消费为点
            // 重试队列则从队列尾部开始
            if (messageQueue.topic.indexOf(MixAll.RETRY_GROUP_TOPIC_PREFIX) === 0) {
              return await this._mqClient.maxOffset(messageQueue);
            }
            // 正常队列则从指定时间点开始
            // 时间点需要参数配置
            const timestamp = utils.parseDate(this.options.consumeTimestamp).getTime();
            return await this._mqClient.searchOffset(messageQueue, timestamp);
          }
          break;
        default:
          break;
      }
      this.logger.info('[mq:consumer] computePullFromWhere() messageQueue => %s should read from offset: %s and lastOffset: %s', messageQueue.key, result, lastOffset);
      return result;
    } catch (err) {
      err.mesasge = 'computePullFromWhere() occurred an exception, ' + err.mesasge;
      this._handleError(err);
      return -1;
    }
  }

  /**
   * 移除消费队列
   * @param {MessageQueue} messageQueue - message queue
   * @return {void}
   */
  async removeProcessQueue(messageQueue) {
    // ---------- modify by zhangbing fix bug begin ----------
    // const processQueue = this._processQueueTable.get(messageQueue.key);
    const item = this._processQueueTable.get(messageQueue.key);
    this._processQueueTable.delete(messageQueue.key);
    const processQueue = item ? item.processQueue : null;
    // ---------- modify by zhangbing fix bug end ----------
    if (processQueue) {
      processQueue.dropped = true;
      await this.removeUnnecessaryMessageQueue(messageQueue, processQueue);
      this.logger.info('[mq:consumer] remove unnecessary messageQueue, %s, Dropped: %s', messageQueue.key, processQueue.dropped);
    }
  }

  /**
   * remove unnecessary queue
   * @param {MessageQueue} messageQueue - message queue
   * @param {ProcessQueue} pq - process queue
   * @return {void}
   */
  async removeUnnecessaryMessageQueue(messageQueue, pq) {
    await this._offsetStore.persist(messageQueue);
    this._offsetStore.removeOffset(messageQueue);
    // todo: consume later ？

    if (this._consumeOrderly && this.messageModel === MessageModel.CLUSTERING) {
      try {
        if (await pq.lockConsume.tryLock(1000)) {
          try {
            const success = await this.unlockDelay(messageQueue, pq);
            return success;
          } finally {
            pq.lockConsume.unlock();
          }
        } else {
          this.logger.warn(`[WRONG]mq is consuming, so can not unlock it, ${messageQueue.key}. maybe hanged for a while, ${pq.tryUnlockTimes}`);
          pq.incTryUnlockTimes();
        }
      } catch (err) {
        this.logger.error('removeUnnecessaryMessageQueue Exception', err);
      }

      return false;
    }
    return true;
  }


  async sendMessageBack(msg, delayLevel, brokerName, consumerGroup) {
    const brokerAddr = brokerName ? this._mqClient.findBrokerAddressInPublish(brokerName) :
      msg.storeHost;
    const thatConsumerGroup = consumerGroup ? consumerGroup : this.consumerGroup;
    try {
      await this._mqClient.consumerSendMessageBack(
        brokerAddr,
        msg,
        thatConsumerGroup,
        delayLevel,
        3000,
        this.options.maxReconsumeTimes);
      this.logger.info('[MQPushConsumer] consumerSendMessageBack success, topic=%s, consumerGroup=%s, msgId=%s', msg.topic, thatConsumerGroup, msg.msgId);
    } catch (err) {
      err.mesasge = 'sendMessageBack() occurred an exception, ' + thatConsumerGroup + ', ' + err.mesasge;
      this._handleError(err);

      let newMsg;
      if (MixAll.isRetryTopic(msg.topic)) {
        newMsg = msg;
      } else {
        newMsg = new Message(MixAll.getRetryTopic(thatConsumerGroup), '', msg.body);
        newMsg.flag = msg.flag;
        newMsg.properties = msg.properties;
        newMsg.originMessageId = msg.originMessageId || msg.msgId;
        newMsg.retryTopic = msg.topic;
        // 这里需要加 1，因为如果 maxReconsumeTimes 为 1，那么这条 retry 消息发出去始终不会被重新投递了
        newMsg.properties[MessageConst.PROPERTY_MAX_RECONSUME_TIMES] = String(this.options.maxReconsumeTimes + 1);
      }

      newMsg.properties[MessageConst.PROPERTY_RECONSUME_TIME] = String(msg.reconsumeTimes + 1);
      newMsg.delayTimeLevel = 0;
      await (await MQProducer.getDefaultProducer()).send(newMsg);
    }
  }

  // * viewMessage(msgId) {
  //   const info = MessageDecoder.decodeMessageId(msgId);
  //   return yield this._mqClient.viewMessage(info.address, Number(info.offset.toString()), 3000);
  // }

  _handleError(err) {
    err.message = 'MQPushConsumer occurred an error ' + err.message;
    this.emit('error', err);
  }

  _sleep(timeout) {
    return sleep(timeout);
  }

  // ------- add by zhangbing begin -----------

  /**
   * @param {boolean} oneway -
   */
  async unlockAll(oneway) {
    /**
     * @type {Map<string, MessageQueue[]>}
     */
    const brokerMqs = this._buildProcessQueueTableByBrokerName();

    for (const [ brokerName, mqs ] of brokerMqs) {
      if (mqs.length === 0) {
        continue;
      }

      const findBrokerResult = this._mqClient.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
      if (findBrokerResult) {
        const requestBody = {
          consumerGroup: this.consumerGroup,
          clientId: this._mqClient.clientId,
          mqSet: mqs,
        };
        requestBody.consumerGroup = this.consumerGroup;
        requestBody.clientId = this._mqClient.clientId;
        requestBody.mqSet = mqs;

        try {
          await this._mqClient.unlockBatchMQ(findBrokerResult.brokerAddr, requestBody, 1000, oneway);

          for (const mq of mqs) {
            const item = this._processQueueTable.get(mq.key);
            if (item) {
              const processQueue = item.processQueue;
              processQueue.locked = false;
              this.logger.info(`the message queue unlock OK, Group: ${this.consumerGroup} ${mq.key}`);
            }
          }
        } catch (err) {
          this.logger.error('unlockBatchMQ exception, ' + JSON.stringify(mqs), err);
        }
      }
    }
  }

  async lockAll() {
    /**
     * @type {Map<String, MessageQueue[]>}
     */
    const brokerMqs = this._buildProcessQueueTableByBrokerName();

    for (const [ brokerName, mqs ] of brokerMqs) {
      if (mqs.length === 0) {
        continue;
      }

      const findBrokerResult = this._mqClient.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
      if (findBrokerResult) {
        const requestBody = {
          consumerGroup: this.consumerGroup,
          clientId: this._mqClient.clientId,
          mqSet: mqs,
        };

        try {
          const lockOKMQSet = await this._mqClient.lockBatchMQ(findBrokerResult.brokerAddr, requestBody, 1000);
          for (const mq of lockOKMQSet) {
            const item = this._processQueueTable.get(mq.key);
            if (item) {
              const processQueue = item.processQueue;
              if (!processQueue.locked) {
                this.logger.info(`the message queue locked OK, Group: ${this.consumerGroup} ${mq.key}`);
              }

              processQueue.locked = true;
              processQueue.lastLockTimestamp = Date.now();
            }
          }
          for (const mq of mqs) {
            if (!lockOKMQSet.find(q => q.key === mq.key)) {
              const item = this._processQueueTable.get(mq.key);
              if (item) {
                const processQueue = item.processQueue;
                processQueue.locked = false;
                this.logger.warn(`the message queue locked Failed, Group: ${this.consumerGroup} ${mq.key}`);
              }
            }
          }
        } catch (err) {
          this.logger.error('lockBatchMQ exception, ' + mqs, err);
        }
      }
    }
  }

  /**
   * @param {MessageQueue} mq -
   * @return {boolean} -
   */
  async lock(mq) {
    const findBrokerResult = this._mqClient.findBrokerAddressInSubscribe(mq.brokerName, MixAll.MASTER_ID, true);
    if (findBrokerResult) {
      const requestBody = {
        consumerGroup: this.consumerGroup,
        clientId: this._mqClient.clientId,
        mqSet: [ mq ],
      };

      try {
        /**
         * @type {MessageQueue[]}
         */
        const lockedMq = await this._mqClient.lockBatchMQ(findBrokerResult.brokerAddr, requestBody, 1000);
        for (const mmqq of lockedMq) {
          const item = this._processQueueTable.get(mmqq.key);
          if (item) {
            const processQueue = item.processQueue;
            processQueue.locked = true;
            processQueue.lastLockTimestamp = Date.now();
          }
        }

        const lockOK = !!lockedMq.find(q => q.key === mq.key);
        this.logger.info(`the message queue lock ${lockOK ? 'OK' : 'Failed'}, ${this.consumerGroup} ${mq.key}`);
        return lockOK;
      } catch (err) {
        this.logger.error('lockBatchMQ exception, ' + mq.key, err);
      }
    }

    return false;
  }

  /**
   * @param {MessageQueue} mq -
   * @param {boolean} oneway -
   * @return {Promise<void>} -
   */
  async unlock(mq, oneway) {
    const findBrokerResult = this._mqClient.findBrokerAddressInSubscribe(mq.brokerName, MixAll.MASTER_ID, true);
    if (findBrokerResult) {
      const requestBody = {
        consumerGroup: this.consumerGroup,
        clientId: this._mqClient.clientId,
        mqSet: [ mq ],
      };

      try {
        await this._mqClient.unlockBatchMQ(findBrokerResult.brokerAddr, requestBody, 1000, oneway);
        this.logger.warn(`unlock messageQueue. group:${this.consumerGroup}, clientId:${this._mqClient.clientId}, mq:${mq.key}`);
      } catch (err) {
        this.logger.error('unlockBatchMQ exception, ' + mq.key, err);
      }
    }
  }

  _buildProcessQueueTableByBrokerName() {
    /**
     * @type {Map<string, MessageQueue[]>}
     */
    const result = new Map();
    for (const [ , item ] of this._processQueueTable) {
      const mq = item.messageQueue;
      let mqs = result.get(mq.brokerName);
      if (!mqs) {
        mqs = [];
        result.set(mq.brokerName, mqs);
      }

      mqs.push(mq);
    }

    return result;
  }

  removeDirtyOffset(mq) {
    this._offsetStore.removeOffset(mq);
  }

  async consumerRunningInfo() {
    const info = { mqTable: { }, properties: {}, statusTable: {}, subscriptionSet: [] };

    const prop = Object.assign(info.properties, this.toRunningProperties());
    prop.PROP_CONSUMEORDERLY = String(this._consumeOrderly);
    prop.PROP_THREADPOOL_CORE_SIZE = String(1);
    prop.PROP_CONSUMER_START_TIMESTAMP = String(this._consumerStartTimestamp);

    const subSet = [];
    for (const [ , item ] of this.subscriptions) {
      subSet.push(item.subscriptionData);
    }

    info.subscriptionSet = subSet;

    for (const [ key, item ] of this._processQueueTable) {
      const mq = item.messageQueue;
      const pq = item.processQueue;

      const pqInfo = {
        cachedMsgCount: 0,
        cachedMsgMaxOffset: 0,
        cachedMsgMinOffset: 0,
        cachedMsgSizeInMiB: 0,
        commitOffset: 0,
        droped: false, // 与服务器交互的数据结构，不能改为dropped
        lastConsumeTimestamp: 0,
        lastLockTimestamp: 0,
        lastPullTimestamp: 0,
        locked: false,
        transactionMsgCount: 0,
        transactionMsgMaxOffset: 0,
        transactionMsgMinOffset: 0,
        tryUnlockTimes: 0,
      };
      const offset = await this._offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
      pqInfo.commitOffset = offset;
      pq.fillProcessQueueInfo(pqInfo);
      info.mqTable[key] = { messageQueue: mq, info: pqInfo };
    }

    // for (const sd of subSet) {
    //   const consumeStatus = this._mqClient.getConsumerStatsManager().consumeStatus(this.consumerGroup, sd.topic);
    //   info.statusTable[sd.topic] = consumeStatus;
    // }

    return info;
  }

  suspend() {
    this._pause = true;
    this.logger.info(`suspend this consumer, ${this.consumerGroup}`);
  }

  async resume() {
    this._pause = false;
    await this.doRebalance();
    this.logger.info(`resume this consumer, ${this.consumerGroup}`);
  }

  /**
   * @param {MessageQueue} mq - message queue
   * @param {ProcessQueue} pq - process queue
   * @return {Promise<boolean>} -
   */
  async unlockDelay(mq, pq) {
    if (pq.hasTempMessage()) {
      this.logger.info(`unlockDelay, begin ${mq.key} `);
      // 延迟处理
      setTimeout(async () => {
        this.logger.info(`unlockDelay, execute at once ${mq.key}`);
        await this.unlock(mq, true);
      }, this.options.unlockDelayTimeMills);
    } else {
      await this.unlock(mq, true);
    }
    return true;
  }

  async messageQueueChanged(topic/* , mqAll, mqDivided */) {
    /**
     * When rebalance result changed, should update subscription's version to notify broker.
     * Fix: inconsistency subscription may lead to consumer miss messages.
     */
    const subscriptionData = this.subscriptions.get(topic);
    const newVersion = Date.now();
    this.logger.info(`${topic} Rebalance changed, also update version: ${subscriptionData.subVersion}, ${newVersion}`);
    subscriptionData.subVersion = newVersion;

    const currentQueueCount = this.processQueueTable.size;
    if (currentQueueCount > 0) {
      const pullThresholdForTopic = this.options.pullThresholdForTopic;
      if (pullThresholdForTopic !== -1) {
        const newVal = Math.max(1, pullThresholdForTopic / currentQueueCount);
        this.logger.info(`The pullThresholdForQueue is changed from ${this.options.pullThresholdForQueue} to ${newVal}`);
        this.options.pullThresholdForQueue = newVal;
      }

      const pullThresholdSizeForTopic = this.options.pullThresholdSizeForTopic;
      if (pullThresholdSizeForTopic !== -1) {
        const newVal = Math.max(1, pullThresholdSizeForTopic / currentQueueCount);
        this.logger.info(`The pullThresholdSizeForQueue is changed from ${this.options.pullThresholdSizeForQueue} to ${newVal}`);
        this.options.pullThresholdSizeForQueue = newVal;
      }
    }

    // notify broker
    await this._mqClient.sendHeartbeatToAllBroker();
  }

  toRunningProperties() {
    return {
      maxReconsumeTimes: String(this.options.maxReconsumeTimes),
      unitMode: String(this.unitMode),
      adjustThreadPoolNumsThreshold: '100000',
      consumerGroup: this.consumerGroup,
      messageModel: this.messageModel,
      allocateMessageQueueStrategy: this.allocateMessageQueueStrategy.fullName,
      suspendCurrentQueueTimeMillis: String(this.suspendCurrentQueueTimeMillis),
      pullThresholdSizeForTopic: String(this.options.pullThresholdSizeForTopic),
      pullThresholdSizeForQueue: String(this.options.pullThresholdSizeForQueue),
      offsetStore: 'org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore',
      consumeConcurrentlyMaxSpan: String(this.options.consumeConcurrentlyMaxSpan),
      postSubscriptionWhenPull: String(this.options.postSubscriptionWhenPull),
      consumeTimestamp: String(this.options.consumeTimestamp),
      consumeTimeout: String(this.consumeTimeout),
      consumeMessageBatchMaxSize: String(this.options.consumeMessageBatchMaxSize),
      defaultMQPushConsumerImpl: 'org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl',
      pullInterval: String(this.options.pullInterval),
      pullThresholdForQueue: String(this.options.pullThresholdForQueue),
      pullThresholdForTopic: String(this.options.pullThresholdForTopic),
      consumeFromWhere: this.consumeFromWhere,
      pullBatchSize: String(this.options.pullBatchSize),
      consumeThreadMin: '1',
      consumeThreadMax: '1',
      subscription: '{}',
      messageListener: 'com.mctech.consumer.ConsumerCommandRunner',
    };
  }
}
// if subscriber return ACTION_RETRY, message will be directly retried
MQPushConsumer.ACTION_RETRY = Symbol('ACTION_RETRY');

// ------- add by zhangbing end -----------

module.exports = MQPushConsumer;

// Helper
// ------------------
function compare(mqA, mqB) {
  if (mqA.topic === mqB.topic) {
    if (mqA.brokerName === mqB.brokerName) {
      return mqA.queueId - mqB.queueId;
    }
    return mqA.brokerName > mqB.brokerName ? 1 : -1;
  }
  return mqA.topic > mqB.topic ? 1 : -1;
}
