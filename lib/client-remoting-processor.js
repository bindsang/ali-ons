'use strict';

const ByteBuffer = require('byte');
// const MQClient = require('./mq_client');
// const MessageConst = require('./message/message_const');
const MessageDecoder = require('./message/message_decoder');
const RequestCode = require('./protocol/request_code');
const ResponseCode = require('./protocol/response_code');
const MessageQueue = require('./message_queue');
const RemotingCommand = require('./protocol/command/remoting_command');

const MQ_REGEX = /\{[^{}]+\}/g;

module.exports = class ClientRemotingProcessor {
  /**
   *
   * @param {Logger} logger -
   * @param {MQClient} mqClient -
   */
  constructor(logger, mqClient) {
    this.logger = logger;
    this._mqClient = mqClient;
  }

  /**
   * @param {address} address -
   * @param {RemotingCommand} request -
   * @return {RemotingCommand} -
   */
  async processRequest(address, request) {
    /**
     * @type {RemotingCommand}
     */
    let result = null;
    switch (request.code) {
      // case RequestCode.CHECK_TRANSACTION_STATE:
      //   return this._checkTransactionState(address, request);
      case RequestCode.GET_CONSUMER_RUNNING_INFO:
        result = await this._getConsumerRunningInfo(address, request);
        break;
      case RequestCode.NOTIFY_CONSUMER_IDS_CHANGED:
        result = await this._notifyConsumerIdsChanged(address, request);
        break;
      case RequestCode.RESET_CONSUMER_CLIENT_OFFSET:
        result = await this._resetOffset(address, request);
        break;
      case RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT:
        result = await this._getConsumeStatus(address, request);
        break;
      case RequestCode.CONSUME_MESSAGE_DIRECTLY:
        result = await this._consumeMessageDirectly(address, request);
        break;
      default:
        break;
    }
    return result;
  }

  get rejectRequest() {
    return false;
  }

  // /**
  //  * @param {address} address -
  //  * @param {RemotingCommand} request -
  //  * @return {RemotingCommand} -
  //  */
  // async _checkTransactionState(address, request) {
  //   const requestHeader = request.decodeCommandCustomHeader();
  //   const byteBuffer = ByteBuffer.wrap(request.body);
  //   const messageExt = MessageDecoder.decode(byteBuffer);
  //   if (messageExt != null) {
  //     const transactionId = messageExt.properties[MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX];
  //     if (transactionId) {
  //       messageExt.transactionId = transactionId;
  //     }
  //     const group = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
  //     if (group) {
  //       const producer = this.mqClient.selectProducer(group);
  //       if (producer) {
  //         producer.checkTransactionState(address, messageExt, requestHeader);
  //       } else {
  //         this.logger.debug(`checkTransactionState, pick producer by group[${group}] failed`);
  //       }
  //     } else {
  //       this.logger.warn('checkTransactionState, pick producer group failed');
  //     }
  //   } else {
  //     this.logger.warn('checkTransactionState, decode message failed');
  //   }

  //   return null;
  // }
  /**
   * @param {address} address -
   * @param {RemotingCommand} request -
   * @return {RemotingCommand} -
   */
  async _notifyConsumerIdsChanged(address, request) {
    try {
      const requestHeader = request.decodeCommandCustomHeader();
      this.logger.info(`receive broker's notification[${address}], the consumer group: ${requestHeader.consumerGroup} changed, rebalance immediately`);
      await this._mqClient.doRebalance();
    } catch (err) {
      this.logger.error('notifyConsumerIdsChanged exception', err.message + ' --> ' + err.stack);
    }
    return null;
  }

  /**
   * @param {address} address -
   * @param {RemotingCommand} request -
   * @return {RemotingCommand} -
   */
  async _resetOffset(address, request) {
    const requestHeader = request.decodeCommandCustomHeader();
    this.logger.info(`invoke reset offset operation from broker. brokerAddr=${address}, topic=${requestHeader.topic}, group=${requestHeader.group}, timestamp=${requestHeader.timestamp}`);
    /**
     * @type {Map<string, number>} MessageQueue -> offset
     */
    const offsetTable = new Map();
    if (request.body) {
      // {"offsetTable":{{"brokerName":"broker-a","queueId":0,"topic":"%RETRY%CID_SAMPLE"}: 25}}
      // java版本使用的是fastjson序列化
      // fastjson在序列化Map<object, object>这样的数据结构时，
      // 生成的json格式不是标准的json，不能使用公共类库生成，只能拼字符串
      // 由于此处返回的字符串格式比较简单，所以直接使用正则表达式提取位于key的位置上的值，转成简单的字符串再用json返序列化
      const str = request.body.toString();
      const mqMap = {};
      let count = 0;
      const jsonText = str.replace(MQ_REGEX, s => {
        const queue = MessageQueue.fromJSON(JSON.parse(s));
        const key = String(count);
        const result = '"' + key + '"';
        mqMap[key] = queue.key;
        count++;
        return result;
      });

      const body = JSON.parse(jsonText);
      for (const key in body.offsetTable) {
        offsetTable.set(mqMap[key], body.offsetTable[key]);
      }
    }
    await this._mqClient.resetOffset(requestHeader.topic, requestHeader.group, offsetTable);
    return null;
  }

  /**
   * @deprecated
   * @param {address} address -
   * @param {RemotingCommand} request -
   * @return {RemotingCommand} -
   */
  _getConsumeStatus(address, request) {
    const response = RemotingCommand.createResponseCommand();
    const requestHeader = request.decodeCommandCustomHeader();

    /**
     * @type {Map<string, {messageQueue: MessageQueue, offset: number}>} MessageQueue -> offset
     */
    const offsetTable = this._mqClient.getConsumerStatus(requestHeader.topic, requestHeader.group);
    // GetConsumerStatusBody
    // java版本使用的是fastjson序列化
    // fastjson在序列化Map<object, object>这样的数据结构时，
    // 生成的json格式不是标准的json，不能使用公共类库生成，只能拼字符串
    const builder = [];
    builder.push('{"consumerTable":{},"messageQueueTable":{');
    let first = true;
    for (const [ , item ] of offsetTable) {
      if (!first) {
        builder.push(',');
        first = false;
      }
      const mq = item.messageQueue;
      builder.push(JSON.stringify(mq.toJSON()));
      builder.push(':');
      builder.push(item.offset);
    }
    builder.push('}}');
    response.body = Buffer.from(builder.join(''));
    response.code = ResponseCode.SUCCESS;
    return response;
  }

  /**
   * @param {address} address -
   * @param {RemotingCommand} request -
   * @return {RemotingCommand} -
   */
  async _getConsumerRunningInfo(address, request) {
    const response = RemotingCommand.createResponseCommand();
    // GetConsumerRunningInfoRequestHeader
    const requestHeader = request.decodeCommandCustomHeader();

    // ConsumerRunningInfo
    const consumerRunningInfo = await this._mqClient.consumerRunningInfo(requestHeader.consumerGroup);
    if (consumerRunningInfo) {
      response.code = ResponseCode.SUCCESS;
      const mqTable = consumerRunningInfo.mqTable;
      // java版本使用的是fastjson序列化
      // fastjson在序列化Map<object, object>这样的数据结构时，
      // 生成的json格式不是标准的json，不能使用公共类库生成，只能拼字符串
      const builder = [];
      builder.push('{');
      let first = true;
      for (const key in mqTable) {
        const { messageQueue, info } = mqTable[key];
        if (first) {
          first = false;
        } else {
          builder.push(',');
        }
        builder.push(JSON.stringify(messageQueue.toJSON()));
        builder.push(':');
        builder.push(JSON.stringify(info));
      }
      builder.push('}');
      consumerRunningInfo.mqTable = '$$PLACE_HOLDER$$';
      let str = JSON.stringify(consumerRunningInfo);
      str = str.replace('"$$PLACE_HOLDER$$"', builder.join(''));
      response.body = Buffer.from(str);
    } else {
      response.code = ResponseCode.SYSTEM_ERROR;
      response.remark = `The Consumer Group <${requestHeader.consumerGroup}> not exist in this consumer`;
    }

    return response;
  }

  /**
   * @param {address} address -
   * @param {RemotingCommand} request -
   * @return {RemotingCommand} -
   */
  async _consumeMessageDirectly(address, request) {
    const response = RemotingCommand.createResponseCommand();
    // ConsumeMessageDirectlyResultRequestHeader
    const requestHeader = request.decodeCommandCustomHeader();

    const msg = MessageDecoder.decode(ByteBuffer.wrap(request.body));
    // ConsumeMessageDirectlyResult
    const result = await this._mqClient.consumeMessageDirectly(msg, requestHeader.consumerGroup, requestHeader.brokerName);
    if (result) {
      response.code = ResponseCode.SUCCESS;
      response.body = result.encode();
    } else {
      response.code = ResponseCode.SYSTEM_ERROR;
      response.remark = `The Consumer Group <${requestHeader.consumerGroup}> not exist in this consumer`;
    }

    return response;
  }
};
