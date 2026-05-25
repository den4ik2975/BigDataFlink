# Отчет по лабораторной работе N3

## Тема

Потоковая обработка данных с помощью Apache Flink: чтение JSON-сообщений из Kafka, трансформация в модель звезда и запись результата в PostgreSQL.

## Цель работы

Реализовать streaming ETL, который:

1. преобразует строки CSV-файлов в JSON-сообщения Kafka;
2. читает Kafka-топик приложением Apache Flink;
3. формирует таблицы фактов и измерений в PostgreSQL.

## Исходные данные

В папке `исходные данные` находится 10 CSV-файлов по 1000 строк. Всего producer отправляет в Kafka 10000 JSON-сообщений.

Данные содержат сведения о клиентах, продавцах, товарах, магазинах, поставщиках и продажах товаров для домашних питомцев.

## Используемые технологии

- Docker Compose;
- PostgreSQL 16;
- Apache Kafka 3.7;
- Apache Flink 1.19.1;
- Java 17;
- Python producer.

## Структура решения

- `docker-compose.yml` - запуск PostgreSQL, Kafka, Flink JobManager/TaskManager и producer.
- `sql/postgres/01_star_schema.sql` - создание таблиц модели звезда.
- `producer/produce_csv_to_kafka.py` - отправка строк CSV в Kafka в формате JSON.
- `flink-job/pom.xml` - Maven-проект Flink job.
- `flink-job/src/main/java/ru/lab/PetSalesFlinkJob.java` - streaming ETL из Kafka в PostgreSQL.

## Соответствие требованиям

| Требование README | Реализация |
| --- | --- |
| 10 исходных CSV-файлов по 1000 строк | Папка `исходные данные`, producer читает все CSV |
| PostgreSQL, Flink и Kafka через Docker Compose | `docker-compose.yml` |
| Приложение CSV -> JSON -> Kafka | `producer/produce_csv_to_kafka.py` |
| Flink streaming job для чтения Kafka | `flink-job/src/main/java/ru/lab/PetSalesFlinkJob.java` |
| Запись результата в PostgreSQL в модель звезда | `sql/postgres/01_star_schema.sql`, JDBC sink внутри Flink job |
| Инструкция запуска и проверки | README и раздел "Запуск" этого отчета |

## Модель данных PostgreSQL

Фактовая таблица:

- `fact_sales` - факт продажи, исходные идентификаторы, количество, сумма, цена единицы и ссылки на измерения.

Измерения:

- `dim_customer` - клиенты;
- `dim_seller` - продавцы;
- `dim_product` - товары;
- `dim_supplier` - поставщики;
- `dim_store` - магазины;
- `dim_date` - календарь продаж;
- `dim_country` - страны;
- `dim_city` - города;
- `dim_postal_area` - почтовые области;
- `dim_product_category` - категории товаров;
- `dim_pet_category` - категории питомцев.

Ключи измерений строятся из естественных атрибутов записи. Это позволяет streaming job выполнять идемпотентные `INSERT ... ON CONFLICT DO NOTHING` без предварительного чтения surrogate key из PostgreSQL.

## Алгоритм выполнения

1. PostgreSQL создает таблицы модели звезда.
2. Producer читает все CSV-файлы, добавляет служебный `source_raw_id` и отправляет каждую строку JSON-сообщением в Kafka-топик `pet-sales`.
3. Flink job читает Kafka через `KafkaSource`.
4. Для каждого события Flink формирует ключи измерений, приводит числовые поля и даты к типам PostgreSQL.
5. В одной транзакции записываются измерения и факт продажи.

## Запуск

```bash
docker compose down -v
docker compose up --build -d
```

Параметры подключения к PostgreSQL:

- host: `localhost`
- port: `5434`
- database: `lab3`
- user: `lab`
- password: `lab`

Flink UI доступен по адресу `http://localhost:8081`.

## Проверка результата

Команды проверки:

```bash
docker compose logs producer
docker compose exec -T postgres psql -U lab -d lab3 -c "SELECT COUNT(*) FROM fact_sales;"
docker compose exec -T postgres psql -U lab -d lab3 -c "SELECT 'dim_country' AS table_name, COUNT(*) FROM dim_country UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales;"
```

Ожидаемые результаты после обработки всех сообщений:

| Таблица | Количество строк |
| --- | ---: |
| `dim_country` | 230 |
| `dim_product` | 10000 |
| `dim_date` | 364 |
| `fact_sales` | 10000 |

## Вывод

Реализован streaming ETL на Apache Flink. Исходные CSV-строки отправляются в Kafka как JSON-сообщения, Flink читает поток и заполняет PostgreSQL-модель звезда для последующего анализа продаж.
