# Запуск в Docker

Нужно: Docker Engine 24+ с Compose v2, ~6 ГБ свободной RAM для Docker, ~5 ГБ на диске
(образ Airflow собирается с Spark 3.5.1 + Java 17), свободные порты
5432, 7077, 8000, 8080, 8081, 8501, 9000, 9001.

## 1. Старт
```bash
cd bank-dwh-mvp
docker compose up --build -d
docker compose ps          # ждём: minio healthy, minio-init/airflow-init Exited (0), остальные Up
```
Первая сборка долгая (скачивание Spark ~400 МБ). Init-скрипты Postgres выполняются
ТОЛЬКО на пустом томе; после правки `mart/init.sql` нужен `docker compose down -v`.

## 2. Проверка, что всё поднялось
```bash
docker compose logs airflow-init | tail -5      # должно закончиться без Traceback
docker compose logs spark-worker | grep -i "registered with master"
curl -s localhost:8000/health                    # {"status":"ok"}
docker compose exec postgres psql -U mart -d mart -c "select count(*) from dq.rule_catalog"   # 10
```
Airflow http://localhost:8081 (admin/admin), Spark http://localhost:8080 (Workers: 1 ALIVE),
MinIO http://localhost:9001 (minioadmin/minioadmin, бакет `datalake`), Dashboard http://localhost:8501.

## 3. Запуск пайплайна
В Airflow включи тумблеры (unpause) у трёх DAG'ов: `extract_raw`, `transform_load_mart`, `data_quality_checks`.
Scheduler сам создаст по одному прогону за вчерашнюю дату с ОДНОЙ logical date у всех трёх —
сенсоры `ExternalTaskSensor` матчатся именно по ней.
НЕ запускай DAG'и кнопкой Trigger: у ручного запуска logical date = «сейчас», разная у каждого DAG,
и сенсор будет ждать 30 минут и упадёт по таймауту.
Повтор за конкретную дату — Clear на нужном run'е, а не Trigger.

Порядок: extract_raw (~1 мин) → transform_load_mart (первый раз 3–10 мин: `--packages` качает
hadoop-aws + AWS SDK) → data_quality_checks.

## 4. Проверка результата
```bash
docker compose exec postgres psql -U mart -d mart -c \
 "select rule_code,status,metric_value from dq.check_results order by check_id desc limit 10"
psql -h localhost -U analyst_ro   -d mart -c "select * from restricted.dim_client_pii"   # permission denied
psql -h localhost -U data_steward -d mart -c "select count(*) from restricted.dim_client_pii"  # ожидаемо 1500 (3 банка × 500 клиентов)
```
(пароль у ролей = имя роли: analyst_ro / data_steward / mart)

## 5. Демо DQ-инцидента
```bash
psql -h localhost -U mart -d mart -v batch_date=<ds прогона, ГГГГ-ММ-ДД> \
     -f data_governance/generate_dq_incidents.sql
```
Затем в Airflow: Clear таска `run_data_quality_checks` за тот же run → DQ-003/005/006 FAIL,
DAG падает (HIGH), инциденты в `dq_incident.incidents`.
`incident_demo.sql` жёстко работает с `incident_id = 1` — проверь, что это нужный инцидент.

## 6. Диагностика
| Симптом | Куда смотреть |
|---|---|
| `dependency failed to start: container ... is unhealthy` | `docker compose logs minio` |
| postgres в Exited при первом старте | `docker compose logs postgres` — ошибка init.sql; затем `down -v` |
| transform падает на spark-submit | лог таска в Airflow; `docker compose logs spark-master spark-worker` |
| `Initial job has not accepted any resources` | воркер не зарегистрировался / мало памяти у Docker |
| сенсор висит в up_for_reschedule | запускал Trigger'ом вместо unpause (см. п. 3) |

Остановка: `docker compose down` (данные остаются), полный сброс: `docker compose down -v`.
