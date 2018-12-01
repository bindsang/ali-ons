'use strict';

const fmt = require('util').format;

class MessageQueue {
  constructor(topic, brokerName, queueId) {
    this.topic = topic;
    this.brokerName = brokerName;
    this.queueId = queueId;

    this.key = fmt('[topic="%s", brokerName="%s", queueId="%s"]', this.topic, this.brokerName, this.queueId);
  }

  toJSON() {
    return {
      brokerName: this.brokerName,
      queueId: this.queueId,
      topic: this.topic,
    };
  }

  static fromJSON(json) {
    return new MessageQueue(json.topic, json.brokerName, json.queueId);
  }
}

module.exports = MessageQueue;
