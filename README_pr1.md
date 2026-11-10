# Практика 1:
## Выполнил Зиннатуллин Данил, студент группы ИВТ-ЦТКС-203М

| Задание | Основной паттерн | Дополнительные паттерны |
|---|---|---|
| [Ремонт скважин](task1-point-to-point/README.md) | Point-to-Point Channel | Guaranteed Delivery |
| [Цена нефти](task2-publish-subscribe/README.md) | Publish-Subscribe Channel | Correlation Identifier, Guaranteed Delivery |

## Запуск из корня репозитория

```powershell
docker compose -f task1-point-to-point/docker-compose.yml up -d --build
docker compose -f task2-publish-subscribe/docker-compose.yml up -d --build

docker compose -f task1-point-to-point/docker-compose.yml ps -a
docker compose -f task2-publish-subscribe/docker-compose.yml ps -a
```
