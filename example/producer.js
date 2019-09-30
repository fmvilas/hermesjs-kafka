#!/usr/bin/env node

const { Producer, KafkaClient } = require('kafka-node');
const client = new KafkaClient({
  kafkaHost: 'localhost:9092',
  clientId: 'ABC',
  requestTimeout: 60000
});
const producer = new Producer(client);

producer.on('error', console.error);

producer.on('ready', () => {
  producer.send([
    { topic: 'trip__requested', messages: 'test' }
  ], (err) => {
    if (err) return console.error(err);
    console.log('Message sent!');
  });
});
