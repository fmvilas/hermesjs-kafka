const { Kafka, logLevel } = require('kafkajs');
const { Adapter, Message } = require('hermesjs');

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
      this.options.topics = this.options.topics || [];
      this.options.brokers = this.options.brokers || ['localhost:9092'];
      this.client = new Kafka({
        ...{ logLevel: logLevel.NOTHING },
        ...this.options
      });

      const consumer = this.client.consumer(this.options.consumerOptions);
      try {
        await consumer.connect();
        connected = true;
        resolve(this);
        await Promise.all(this.options.topics.map(topic => consumer.subscribe({ topic })));
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
    if (this.options.topicSeparator === undefined) return topicName;
    return topicName.replace(new RegExp(`${this.options.topicSeparator}`, 'g'), '/');
  }

  _translateTopicNames (topicNames) {
    const topics = Array.isArray(topicNames) ? topicNames : [topicNames];
    return topics.map(this._translateTopicName);
  }

  _translateHermesRoute (hermesRoute) {
    if (this.options.topicSeparator === undefined) return hermesRoute;
    return hermesRoute.replace(/\//g, this.options.topicSeparator);
  }
}

module.exports = KafkaAdapter;
