const Hermes = require('hermesjs');
const KafkaAdapter = require('..');

const hermes = new Hermes();

hermes.addAdapter(KafkaAdapter, {
  kafkaHost: 'localhost:9092',
  groupId: 'test',
  topics: ['trip__requested', 'trip__accepted'],
  topicSeparator: '__',
});

hermes.use('trip/requested', (message, next) => {
  console.log('Trip requested');
  hermes.send('test', {}, 'trip/accepted');
  next();
});

hermes.use('trip/accepted', (message, next) => {
  console.log('Trip accepted');
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
