# hermesjs-kafka

Kafka adapter for [HermesJS](https://github.com/fmvilas/hermes).

## Installing

```
npm install hermesjs-kafka
```

## Example

```js
const Hermes = require('hermesjs');
const KafkaAdapter = require('hermesjs-kafka');

const app = new Hermes();

app.addAdapter(KafkaAdapter, {
  clientId: 'myClientId',
  brokers: ['localhost:9092'],
  consumerOptions: {
    groupId: 'myGroupId',
  },
  topics: ['user__signedup'],
  topicSeparator: '__',
});
```

See a working example [here](./example/index.js).

## Author

Fran Méndez ([fmvilas.com](https://fmvilas.com))
