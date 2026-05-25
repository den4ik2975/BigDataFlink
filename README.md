# BigDataFlink
Анализ больших данных - лабораторная работа №3 - Streaming processing с помощью Flink

Одним из самых популярных фреймворков для работы со streaming processing является Apache Flink. Apache Flink - мощный фреймворк, который предлагает широкий набор функциональности для простого написания streaming processing.

Что необходимо сделать? 

Необходимо реализовать потоковую обработку данных с помощью Flink, который читает топик Kafka, трансформирует данные в режиме streaming в модель звезда и пишет результат в PostgreSQL. Данные в Kafka-топиках хранятся в формате json. Данные в топик kafka нужно отправлять самостоятельно, эмулируя источник данных.

Какие данные отправляются в Kafka?
 - Каждое сообщение в Kafka-топике - это строчка из csv файлов, преобразованная в формат json.

Какие данные отправляются в PostgreSQL?
 - Трансформированные данные в модель данных звезда.

![Лабораторная работа №3](https://github.com/user-attachments/assets/d3c1544d-3fe6-4c15-b673-9aa5d27dbd76)


Алгоритм:

1. Клонируете к себе этот репозиторий.
2. Устанавливаете инструмент для работы с запросами SQL (рекомендую DBeaver).
3. Устанавливаете базу данных PostgreSQL (рекомендую установку через docker).
4. Устанавливаете Apache Flink (рекомендую установку через Docker).
5. Устанавливаете Apache Kafka (рекомендую установку через Docker).
6. Скачиваете файлы с исходными данными mock_data( * ).csv, где ( * ) номера файлов. Всего 10 файлов, каждый по 1000 строк.
7. Реализуете приложение, которое каждую строчку из исходных csv-файлов преобразует в json и отправляет в виде сообщения в Kafka-топик.
8. Реализуете приложение на Flink, которое читает Kafka-топик, преобразует данные в модель звезда и сохраняет в PostgreSQL в режиме streaming.
9. Проверяете конечные данные в PostgreSQL.
10. Отправляете работу на проверку лаборантам.

Что должно быть результатом работы?

1. Репозиторий, в котором есть исходные данные mock_data().csv, где () номера файлов. Всего 10 файлов, каждый по 1000 строк.
2. Файл docker-compose.yml с установкой PostgreSQL, Flink, Kafka и запуском приложения, которое из файлов mock_data(*).csv создает сообщения json в Kafka.
3. Инструкция, как запускать Flink-джобу и приложение для отправки данных в Kafka для проверки лабораторной работы.
4. Код Apache Flink для трансформации данных в режиме streaming.

## Решение

Выполнен обязательный объем лабораторной:

1. `docker-compose.yml` - PostgreSQL 16, Kafka 3.7, Flink 1.19.1 и producer CSV -> JSON -> Kafka.
2. `sql/postgres/01_star_schema.sql` - DDL модели звезда в PostgreSQL.
3. `producer/produce_csv_to_kafka.py` - приложение, которое читает 10 CSV-файлов и отправляет каждую строку JSON-сообщением в Kafka-топик `pet-sales`.
4. `flink-job/src/main/java/ru/lab/PetSalesFlinkJob.java` - Apache Flink streaming job: читает Kafka, преобразует JSON в измерения и факт продаж, пишет результат в PostgreSQL.

Модель содержит факт `fact_sales` и измерения:
`dim_customer`, `dim_seller`, `dim_product`, `dim_supplier`, `dim_store`, `dim_date`, `dim_country`, `dim_city`, `dim_postal_area`, `dim_product_category`, `dim_pet_category`.

## Запуск

Для чистого запуска:

```bash
docker compose down -v
docker compose up --build -d
```

При старте:

- PostgreSQL создает таблицы модели звезда;
- Kafka поднимает топик `pet-sales`;
- producer отправляет 10000 JSON-сообщений из 10 CSV-файлов;
- Flink job читает Kafka в streaming-режиме и заполняет PostgreSQL.

PostgreSQL для DBeaver:

- host: `localhost`
- port: `5434`
- database: `lab3`
- user: `lab`
- password: `lab`

Flink UI:

- URL: `http://localhost:8081`

Kafka доступна внутри Docker-сети как `kafka:9092`, с хоста порт проброшен на `localhost:9094`.

## Проверка

Логи producer:

```bash
docker compose logs producer
```

Проверка итоговых таблиц PostgreSQL:

```bash
docker compose exec -T postgres psql -U lab -d lab3 -c "SELECT COUNT(*) FROM fact_sales;"
docker compose exec -T postgres psql -U lab -d lab3 -c "SELECT 'dim_country' AS table_name, COUNT(*) FROM dim_country UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales;"
```
