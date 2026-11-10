# Задание 2: рассылка цены нефти

**Publish-Subscribe Channel**: отдел трейдинга выпускает одну новость;
каждый независимый отдел получает отдельную копию.

```text
                              ┌→ price_news_Бухгалтерия → accounting_subscriber
publisher → price_news fanout ├→ price_news_Нефтебаза → oil_depot_subscriber
                              └→ price_news_Аналитический_отдел → analytics_subscriber
```

`common/topology.js` объявляет durable fanout-exchange `price_news`,
три именованные durable-очереди и три привязки. Publisher и подписчики
используют эту одинаковую идемпотентную декларацию.
Общая очередь на три отдела распределяла бы сообщения вместо рассылки.

Publisher **до отправки** дожидается создания всех очередей и привязок.
Поэтому подписчиков можно запустить позже; фиксированный sleep и ожидание
обработки всеми отделами не нужны. Отключение одного отдела не задерживает других.

**Guaranteed Delivery**: устойчивые exchange/очереди, persistent-сообщения,
ConfirmChannel и mandatory в `common/runtime.js`, ручные ACK после обработки
в `subscriber/subscriber.js`, отдельный volume брокера со стабильным именем узла.

**Correlation Identifier**: `publisher/publisher.js` генерирует `randomUUID()`
на каждый запуск и устанавливает одинаковые значения в:
- `message_id` тела JSON;
- AMQP `messageId` (протокольное `message_id`);
- AMQP `correlationId` (протокольное `correlation_id`).

Подписчик проверяет согласованность всех трёх значений и логирует UUID как
`correlation_id`. Это учебное сопоставление публикации и её копий в логах,
**без отдельного сценария «запрос — ответ»**.

## Данные, события и логи

Одна публикация на запуск:

```json
{
  "message_id": "<новый UUID>",
  "symbol": "BRENT",
  "price": 75.5,
  "timestamp": "<new Date().toISOString()>"
}
```

После подготовки топологии publisher отправляет новость, дожидается confirm,
логирует успех и завершается с кодом 0. Подписчики выводят отдел, символ, цену,
время создания и Correlation ID, отправляют ACK своей копии и ждут новые сообщения.
Обработчики событий и ошибок общие; отличия экземпляров задают
`DEPARTMENT_NAME` и `SERVICE_NAME`.

Время `published_at` из тела одинаково у всех трёх копий. Начальная временная метка
строки — локальное время события. Подтверждение публикации не подтверждает
завершение работы отделов; ACK отделов независимы.

Пример публикации на свежем брокере до запуска подписчиков с текущей формулировкой:

```text
2026-10-27T17:01:28.303Z INFO [publisher] Канал рассылки подготовлен | exchange=price_news type=fanout queues=3
2026-10-27T17:01:28.362Z INFO [publisher] Изменение котировки опубликовано | symbol=BRENT price=75.5 published_at=2026-10-27T17:01:28.305Z correlation_id=97a596e7-7ba6-451a-826b-5bb9637e0be8
```

## Запуск и наблюдение

Из корня репозитория:

```powershell
docker compose -f task2-publish-subscribe/docker-compose.yml up -d --build
docker compose -f task2-publish-subscribe/docker-compose.yml ps -a
docker compose -f task2-publish-subscribe/docker-compose.yml logs -f --timestamps publisher accounting_subscriber oil_depot_subscriber analytics_subscriber
docker compose -f task2-publish-subscribe/docker-compose.yml restart publisher

docker exec rabbitmq-task2 rabbitmqctl -q list_queues name messages_ready messages_unacknowledged --formatter=json
docker compose -f task2-publish-subscribe/docker-compose.yml stop
```

Ctrl+C выходит из просмотра логов. Management UI: http://localhost:15673,
guest / guest. Имена контейнеров: `rabbitmq-task2`, `task2-publisher`,
`task2-accounting`, `task2-oil-depot`, `task2-analytics`.
Build context — папка задания: Dockerfile копирует общие runtime и topology.

## Проверка временно отключённого отдела

После завершения предыдущей новости:

```powershell
docker compose -f task2-publish-subscribe/docker-compose.yml stop accounting_subscriber
docker compose -f task2-publish-subscribe/docker-compose.yml restart publisher
# Дождаться Exited (0); два работающих отдела получат новость
docker compose -f task2-publish-subscribe/docker-compose.yml logs --since 2m publisher oil_depot_subscriber analytics_subscriber
docker exec rabbitmq-task2 rabbitmqctl -q list_queues name messages_ready messages_unacknowledged --formatter=json
# Для Бухгалтерии ready=1, для остальных ready=0; везде unacked=0

docker compose -f task2-publish-subscribe/docker-compose.yml start accounting_subscriber
docker compose -f task2-publish-subscribe/docker-compose.yml logs --since 2m accounting_subscriber
# Доставлена та же новость; все очереди опустели
```

## Публикация до первого запуска подписчиков

На свежих ресурсах проекта можно сначала выполнить:

```powershell
docker compose -f task2-publish-subscribe/docker-compose.yml up -d --build rabbitmq publisher
# Дождаться Exited (0) у publisher; каждая очередь ready=1 / unacked=0
docker exec rabbitmq-task2 rabbitmqctl -q list_queues name messages_ready messages_unacknowledged --formatter=json
docker compose -f task2-publish-subscribe/docker-compose.yml up -d --no-deps accounting_subscriber oil_depot_subscriber analytics_subscriber
# После обработки ready=0 / unacked=0 во всех очередях
```

`--no-deps` позволяет запустить только подписчиков и не перезапускать
завершившийся publisher. Существующие volumes не удаляйте ради проверки без
намеренного решения очистить данные.

При сбое до ACK копия может прийти повторно. Correlation ID помогает
сопоставлять события и не обеспечивает дедупликацию. Общие правила остановки,
`up -d`, `down`, надёжности и интерпретации логов —
[корневой README](../README.md).

