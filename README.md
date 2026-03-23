# Building Automated Data Pipelines (homework)

Airflow **3.1.2** in Docker: DAG `weather_processor` (`airflow/dags/weather.py`) — daily OpenWeatherMap timemachine → SQLite `./airflow/weather.db`.

## Run

```bash
docker compose up -d
docker compose logs -f airflow
```

[http://localhost:8080](http://localhost:8080) · `docker compose down`

## Login

User `admin`. Password:

```bash
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

## Configure (UI)

- Variable **`WEATHER_API_KEY`** — OpenWeather key  
- Connection **`weather_conn_http`** — HTTP, `https` + `api.openweathermap.org`  
- **`weather_conn`** — from Compose (`AIRFLOW_CONN_WEATHER_CONN`); use JSON + host `/opt/airflow/weather.db`, not `sqlite://` URIs

## DAG (short)

`@daily`, catchup from 2026-03-20 UTC; cities Lviv, Kolomyia, Kyiv, Kharkiv, Odesa; chain geocode → timemachine → `measures` table.