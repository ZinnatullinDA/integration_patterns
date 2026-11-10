import { setTimeout as delay } from 'node:timers/promises';
import { run } from './runtime.js';

await run(async rt => {
  const brigade = process.env.BRIGADE_NAME;
  if (!brigade) throw new Error('Не задана BRIGADE_NAME');
  const channel = await rt.connect();
  await channel.assertQueue('repair_requests', { durable: true });
  await channel.prefetch(1);
  await rt.consume('repair_requests', async message => {
    const request = JSON.parse(message.content.toString('utf8'));
    if (!request.request_id || !request.well_id || !request.description) {
      throw new Error('В заявке отсутствуют обязательные поля');
    }
    const fields = { brigade, request: request.request_id, well: request.well_id };
    rt.log('Заявка принята бригадой', { ...fields, redelivered: message.fields.redelivered });
    rt.log('Работы на скважине начаты', fields);
    await delay(2000);
    rt.log('Ремонт завершён', fields);
    channel.ack(message);
    rt.log('Подтверждение обработки отправлено', { ...fields, ack: true });
  });
  rt.log('Бригада готова принимать работы', { brigade, queue: 'repair_requests' });
  await rt.stopPromise;
});

