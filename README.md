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
  kafkaHost: 'localhost:9092',
  topics: ['test'],
});
```

See a working example [here](./example/index.js).

## Author

Fran Méndez ([fmvilas.com](https://fmvilas.com))
