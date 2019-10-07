const { Kafka, logLevel } = require('kafkajs');
const { Adapter, Message } = require('hermesjs');
const uuid = require('uuid/v1');

class KafkaAdapter extends Adapter {
  name () {
    return 'Kafka adapter';
  }

  async connect () {
    return this._connect();
  }

  async send (message, options) {
    return this._send(message, options);
  }

  _connect () {
    return new Promise(async (resolve, reject) => {
      let connected = false;
      this.options = this.options || {};
      this.options.topics = this.options.topics || [];
      this.options.brokers = this.options.brokers || ['localhost:9092'];
      this.client = new Kafka({
        ...{ logLevel: logLevel.NOTHING },
        ...this.options
      });

      this.options.consumerOptions = this.options.consumerOptions || {};
      this.options.consumerOptions.groupId = this.options.consumerOptions.groupId || uuid();

      const consumer = this.client.consumer(this.options.consumerOptions);
      try {
        await consumer.connect();
        await Promise.all(this.options.topics.map(topic => consumer.subscribe({ topic: `${this.options.topicPrefix || ''}${topic}` })));
        connected = true;
        resolve(this);
        await consumer.run({
          eachMessage: async ({ topic, message }) => {
            const msg = this._createMessage(topic, message);
            this.emit('message', msg);
          },
        });
      } catch (e) {
        if (!connected) return reject(e);
        this.emit('error', e);
      }
    });
  }

  async _send (message) {
    try {
      const kafka = new Kafka({
        ...{ logLevel: logLevel.NOTHING },
        ...this.options
      });
      const producer = kafka.producer(this.options.producerOptions);
      await producer.connect();
      const msg = { value: message.payload };
      if (message.headers && message.headers.key) msg.key = message.headers.key;
      await producer.send({
        topic: this._translateHermesRoute(message.topic),
        messages: [msg],
      });
    } catch (e) {
      this.emit('error', e);
    }
  }

  _createMessage (topic, msg) {
    const headers = {
      key: msg.key,
    };

    return new Message(this.hermes, msg.value, headers, this._translateTopicName(topic));
  }

  _translateTopicName (topicName) {
    let result = topicName;
    if (typeof this.options.topicPrefix === 'string' && result.startsWith(this.options.topicPrefix)) {
      result = result.substr(this.options.topicPrefix.length);
    }

    if (typeof this.options.topicSeparator === 'string') {
      result = result.replace(new RegExp(`${this.options.topicSeparator}`, 'g'), '/');
    }

    return result;
  }

  _translateTopicNames (topicNames) {
    const topics = Array.isArray(topicNames) ? topicNames : [topicNames];
    return topics.map(this._translateTopicName);
  }

  _translateHermesRoute (hermesRoute) {
    let result = hermesRoute;
    if (this.options.topicSeparator !== undefined) {
      result = hermesRoute.replace(/\//g, this.options.topicSeparator);
    }

    result = `${this.options.topicPrefix || ''}${result}`;

    return result;
  }
}

module.exports = KafkaAdapter;
