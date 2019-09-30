const Hermes = require('hermesjs');
const KafkaAdapter = require('..');

const hermes = new Hermes();

hermes.addAdapter(KafkaAdapter, {
  kafkaHost: 'localhost:9092',
  groupId: 'test',
  topics: ['test'],
});

hermes.use('test', (message, next) => {
  console.log(message);
  next();
});

hermes.use((err, message, next) => {
  console.log('ERROR', err);
  next();
});

hermes
  .listen()
  .then(() => {
    console.log('Listening...');
  })
  .catch(console.error);
