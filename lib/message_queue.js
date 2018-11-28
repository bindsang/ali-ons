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
    return JSON.stringify({
      topic: this.topic,
      brokerName: this.brokerName,
      queueId: this.queueId,
    });
  }

  static fromJSON(str) {
    const json = JSON.parse(str);
    return new MessageQueue(json.topic, json.brokerName, json.queueId);
  }
}

module.exports = MessageQueue;
