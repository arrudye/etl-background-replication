# ETL-пайплайн 
Для репликации баз данных из PostgreSQL в MongoDB в фоновом режиме.

*Папки mongo_data и pg_data в .gitignore

## Дополнительные задания
- Добавлена третья таблица products, связь усложнена до orders ↔ products (многие-ко-многим)
- Все настройки (пароли, интервал логирования) вынесены в .env файл
- Реализовано отслеживание удалений: добавлено поле deleted_at (soft delete)
- На базовом уровне добавлена идемпотентность

## Запуск:
### Файл `.env`:
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=shop
POSTGRES_USER=admin
POSTGRES_PASSWORD=secret

MONGO_HOST=mongodb
MONGO_PORT=27017
MONGO_DB=shop_replica
MONGO_COLLECTION=customers

SYNC_INTERVAL=60
LOG_LEVEL=INFO

### Запуск контейнеров:
`docker-compose up --build`

