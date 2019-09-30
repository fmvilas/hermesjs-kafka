const { ConsumerGroup, KafkaClient, Producer } = require('kafka-node');
const { Adapter, Message } = require('hermesjs');

class KafkaAdapter extends Adapter {
  async connect () {
    return this._connect();
  }

  async send (message, options) {
    return this._send(message, options);
  }

  _connect () {
    return new Promise((resolve, reject) => {
      let resolved = false;
      this.options.kafkaHost = this.options.kafkaHost || 'localhost:9092';
      this.options.topics = Array.isArray(this.options.topics) ? this.options.topics : [this.options.topics];
      const consumerGroup = new ConsumerGroup(this.options, this.options.topics || []);
      this.client = consumerGroup.client;

      consumerGroup.client.on('ready', () => {
        resolve(this);
        resolved = true;
      });
      consumerGroup.client.on('error', (error) => {
        this.emit('error', error);
        if (!resolved) reject(error);
      });
      consumerGroup.on('error', (error) => {
        this.emit('error', error);
      });
      consumerGroup.on('message', (message) => {
        const msg = this._createMessage(message);
        this.emit('message', msg);
      });
    });
  }

  _send (message) {
    return new Promise((resolve, reject) => {
      const client = new KafkaClient(this.options);
      const producer = new Producer(client);
      producer.on('ready', () => {
        producer.send([
          { topic: message.topic, key: message.headers.key, messages: message.payload }
        ], (err) => {
          if (err) return reject(err);
          resolve();
        });
      });

      producer.on('error', (err) => {
        reject(err);
      });
    });
  }

  _createMessage (msg) {
    const headers = {
      key: msg.key,
    };

    return new Message(this.hermes, msg.value, headers, msg.topic);
  }
}

module.exports = KafkaAdapter;
