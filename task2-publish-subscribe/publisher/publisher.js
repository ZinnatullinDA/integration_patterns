import { randomUUID } from 'node:crypto';
import { run } from './runtime.js';
import { declareTopology, EXCHANGE } from './topology.js';

await run(async rt => {
  const channel = await rt.connect(true);
  rt.log('Связь с брокером установлена', { exchange: EXCHANGE });
  await declareTopology(channel);
  rt.log('Канал рассылки подготовлен', { exchange: EXCHANGE, type: 'fanout', queues: 3 });
  const news = {
    message_id: randomUUID(),
    symbol: 'BRENT',
    price: 75.5,
    timestamp: new Date().toISOString()
  };
  await rt.publish(EXCHANGE, '', news, {
    messageId: news.message_id, correlationId: news.message_id
  });
  rt.log('Изменение котировки опубликовано', {
    symbol: news.symbol, price: news.price, published_at: news.timestamp,
    correlation_id: news.message_id
  });
  rt.log('Выпуск новости завершён', { count: 1, correlation_id: news.message_id });
});

