import { run } from './runtime.js';
import { declareTopology, DEPARTMENTS, queueFor } from './topology.js';

await run(async rt => {
  const department = process.env.DEPARTMENT_NAME;
  if (!DEPARTMENTS.includes(department)) throw new Error('Неизвестная DEPARTMENT_NAME');
  const queue = queueFor(department);
  const channel = await rt.connect();
  await declareTopology(channel);
  await channel.prefetch(1);
  await rt.consume(queue, async message => {
    const news = JSON.parse(message.content.toString('utf8'));
    if (!news.message_id || news.message_id !== message.properties.messageId ||
        news.message_id !== message.properties.correlationId) {
      throw new Error('Идентификаторы JSON, messageId и correlationId не согласованы');
    }
    if (news.symbol !== 'BRENT' || !Number.isFinite(news.price) ||
        !Number.isFinite(Date.parse(news.timestamp))) {
      throw new Error('Некорректная котировка');
    }
    rt.log('Котировка получена отделом', {
      department, symbol: news.symbol, price: news.price, published_at: news.timestamp,
      correlation_id: news.message_id, redelivered: message.fields.redelivered
    });
    channel.ack(message);
    rt.log('Подтверждение обработки отправлено', {
      department, correlation_id: news.message_id, ack: true
    });
  });
  rt.log('Отдел подключён к рассылке', { department, queue });
  rt.log('Ожидание изменений котировки', { department });
  await rt.stopPromise;
});

