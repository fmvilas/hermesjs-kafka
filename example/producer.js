#!/usr/bin/env node

const { Kafka } = require('kafkajs');
const client = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'ABC',
});
const producer = client.producer();

const run = async () => {
  try {
    await producer.connect();
    await producer.send({
      topic: 'trip__requested',
      messages: [{ value: 'test' }],
    });
    console.log('Message sent!');
  } catch (e) {
    console.error(e);
  }
};

run();
