export const EXCHANGE = 'price_news';
export const DEPARTMENTS = ['Бухгалтерия', 'Нефтебаза', 'Аналитический_отдел'];
export const queueFor = department => `price_news_${department}`;

export async function declareTopology(channel) {
  await channel.assertExchange(EXCHANGE, 'fanout', { durable: true });
  for (const department of DEPARTMENTS) {
    const queue = queueFor(department);
    await channel.assertQueue(queue, { durable: true });
    await channel.bindQueue(queue, EXCHANGE, '');
  }
}

