# Airflow setup

The lecture PDF references Apache Airflow `2.0.1`, but this workspace is pinned to Airflow `3.1.2` because you said you need Airflow 3 for ongoing configuration and DAG work.

## Start

```bash
docker compose up -d
docker compose logs -f airflow
```

Open `http://localhost:8080` when the webserver is ready.

This is suitable for local development: creating DAGs, testing connections, adjusting config, and iterating on plugins. It is not a production deployment model.

There are **two** databases in this homework setup:

- **Postgres** (`postgres` service) holds **Airflow’s metadata** (DAG runs, users, variables, etc.). Airflow 3 `standalone` starts several processes that all talk to that DB at once; **SQLite is not reliable there**, which is why the UI can show “can’t be reached” if metadata is on SQLite.
- **SQLite** stores **your own data** in `./airflow/weather.db` (`/opt/airflow/weather.db` in the container). Compose defines `AIRFLOW_CONN_WEATHER_CONN` as JSON (`conn_type` **sqlite**, **host** `/opt/airflow/weather.db`). Do not use a `sqlite:////…` URI for that env var: Airflow’s sqlite hook can rewrite it into an invalid `file://opt/…` URI (`invalid uri authority: opt`). In the UI, use type **SQLite** and **Host** = `/opt/airflow/weather.db` only (no `sqlite://` prefix).

The container uses **`LocalExecutor`** with Postgres metadata, which matches a normal local Airflow 3 dev stack.

## Credentials

Airflow `standalone` creates an admin account automatically. After startup, print the generated password with:

```bash
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

Username: `admin`

## Stop

```bash
docker compose down
```
