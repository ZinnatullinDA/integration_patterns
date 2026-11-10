import { run } from './runtime.js';

await run(async rt => {
  const channel = await rt.connect(true);
  await channel.assertQueue('repair_requests', { durable: true });
  rt.log('Связь с брокером установлена', { queue: 'repair_requests' });
  let count = 0;
  for (let i = 1; i <= 5 && !rt.stopping; i++) {
    const request = {
      request_id: `REQ-${i}`,
      well_id: `WELL-${100 + i}`,
      description: `Плановый ремонт скважины ${100 + i}`
    };
    await rt.publish('', 'repair_requests', request, { messageId: request.request_id });
    count++;
    rt.log('Заявка зарегистрирована в очереди', {
      request: request.request_id, well: request.well_id
    });
    if (i < 5) await rt.pause(1000);
  }
  rt.log('Партия заявок опубликована', { count });
});

