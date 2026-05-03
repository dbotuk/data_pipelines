# Building Automated Data Pipelines (homework)

Airflow **3.1.2** in Docker: DAG `weather_processor` (`airflow/dags/weather.py`) — daily OpenWeatherMap timemachine → Postgres table `measures`.

## Run

```bash
docker compose up -d
docker compose logs -f airflow-scheduler airflow-worker
```

[Airflow UI](http://localhost:8080) · [Flower](http://localhost:5555) · `docker compose down`

## Login

User `admin`. Password `admin`.

## Celery / Flower

The Docker Compose stack uses `CeleryExecutor` with Redis as the broker, Postgres as the metadata/result/data database, and one Airflow worker. Flower is exposed on [http://localhost:5555](http://localhost:5555).

## Configure (UI)

- Variable **`WEATHER_API_KEY`** — OpenWeather key  
- Connection **`weather_conn_http`** — HTTP, `https` + `api.openweathermap.org`  
- **`weather_conn`** — from Compose (`AIRFLOW_CONN_WEATHER_CONN`); points to the Postgres service

## DAG (short)

`@daily`, catchup from 2026-03-20 UTC; cities Lviv, Kolomyia, Kyiv, Kharkiv, Odesa; chain geocode → timemachine → `measures` table.
