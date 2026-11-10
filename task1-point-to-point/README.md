# Задание 1: заявки на ремонт скважин

**Point-to-Point Channel**: диспетчерская публикует заявки в одну очередь.
Два экземпляра одного consumer-кода конкурируют за доставки; бригады не получают
копии каждой заявки.

```text
producer → exchange "" / routing key repair_requests
         → durable queue repair_requests
         → consumer1 (Бригада-1) ИЛИ consumer2 (Бригада-2)
```

**Guaranteed Delivery**: `producer/producer.js` объявляет durable-очередь,
`common/runtime.js` создаёт ConfirmChannel и публикует persistent-сообщение
с mandatory. Брокер подтверждает публикацию; return или отрицательный confirm
приводит к ошибке. В `consumer/consumer.js` используются
`prefetch(1)`, `noAck: false`, асинхронная обработка и `ack` после её завершения.
Именованный volume брокера и стабильное имя узла сохраняют ожидающие заявки.

## Данные и порядок событий

Producer отправляет ровно пять корректных сообщений, интервал между ними 1 с.
REQ-1…REQ-5 относятся к WELL-101…WELL-105.

```json
{
  "request_id": "REQ-1",
  "well_id": "WELL-101",
  "description": "Плановый ремонт скважины 101"
}
```

Каждая бригада разбирает JSON, выводит принятие и начало работ, асинхронно ждёт
2 с, выводит завершение, отправляет ACK и ждёт следующую заявку.
Распределение не фиксировано: первую заявку может получить любая бригада.
Producer ждёт готовности обеих бригад через healthcheck подписки,
но не ждёт завершения ремонта. После пяти confirms он завершается с кодом 0.

Пример штатных логов:

```text
2026-10-27T17:01:34.002Z INFO [producer] Заявка зарегистрирована в очереди | request=REQ-1 well=WELL-101
2026-10-27T17:01:35.008Z INFO [consumer1] Заявка принята бригадой | brigade=Бригада-1 request=REQ-2 well=WELL-102 redelivered=false
2026-10-27T17:01:35.009Z INFO [consumer1] Работы на скважине начаты | brigade=Бригада-1 request=REQ-2 well=WELL-102
2026-10-27T17:01:37.010Z INFO [consumer1] Ремонт завершён | brigade=Бригада-1 request=REQ-2 well=WELL-102
2026-10-27T17:01:37.012Z INFO [consumer1] Подтверждение обработки отправлено | brigade=Бригада-1 request=REQ-2 well=WELL-102 ack=true
```

В этом запуске Бригада-2 обработала REQ-1, REQ-3, REQ-5, а Бригада-1 —
REQ-2, REQ-4. Это наблюдение, а не гарантированная схема распределения.

## Запуск и наблюдение

Все команды выполняются **из корня репозитория**:

```powershell
docker compose -f task1-point-to-point/docker-compose.yml up -d --build
docker compose -f task1-point-to-point/docker-compose.yml ps -a
docker compose -f task1-point-to-point/docker-compose.yml logs -f --timestamps producer consumer1 consumer2
docker compose -f task1-point-to-point/docker-compose.yml restart producer

# Точные счётчики RabbitMQ: оба должны стать 0
docker exec rabbitmq-task1 rabbitmqctl -q list_queues name messages_ready messages_unacknowledged --formatter=json

docker compose -f task1-point-to-point/docker-compose.yml stop
```

Ctrl+C выходит из просмотра логов. Management UI: http://localhost:15672,
guest / guest; очередь `repair_requests`. Названия контейнеров:
`rabbitmq-task1`, `task1-producer`, `task1-consumer1`, `task1-consumer2`.
Переменные `SERVICE_NAME` и `BRIGADE_NAME` различают экземпляры общего кода.
Build context — папка задания, чтобы Dockerfile мог скопировать `common/runtime.js`.

## Проверка устойчивого хранения

После завершения штатного запуска:

```powershell
docker compose -f task1-point-to-point/docker-compose.yml stop consumer1 consumer2
docker compose -f task1-point-to-point/docker-compose.yml restart producer
# Дождаться Exited (0) у producer; очередь должна содержать ready=5 / unacked=0
docker compose -f task1-point-to-point/docker-compose.yml ps -a
docker exec rabbitmq-task1 rabbitmqctl -q list_queues name messages_ready messages_unacknowledged --formatter=json

docker compose -f task1-point-to-point/docker-compose.yml restart rabbitmq
# Дождаться healthy; очередь по-прежнему должна содержать ready=5 / unacked=0
docker compose -f task1-point-to-point/docker-compose.yml ps -a
docker exec rabbitmq-task1 rabbitmqctl -q list_queues name messages_ready messages_unacknowledged --formatter=json

docker compose -f task1-point-to-point/docker-compose.yml start consumer1 consumer2
# После обработки проверить ready=0 / unacked=0 и пять завершений в новом интервале логов
```

Повторные REQ-1…REQ-5 анализируются отдельно от предыдущей партии.
Штатная очередь распределяет одну заявку одному потребителю, но при сбое
до ACK возможна повторная доставка. Бизнес-дедупликация и блокировка работ
по `well_id` за пределами задания. Общие правила остановки, `up -d`, `down`,
границы гарантий и пояснения логов — в [корневом README](../README.md).

