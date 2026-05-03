import json
import logging

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.sdk import TaskGroup
from airflow.task.trigger_rule import TriggerRule
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from datetime import timedelta


CITIES = ["Lviv", "Kolomyia", "Kyiv", "Kharkiv", "Odesa"]
WIND_SPEED_ALERT_THRESHOLD = 10
HTTP_RETRY_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}
DB_RETRY_ARGS = {
    "retries": 5,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
}


def _fetch_weather(geolocation, api_key, timestamp):
    if not geolocation:
        raise AirflowException("Geolocation response is empty; cannot fetch weather data.")

    location = geolocation[0]
    if "lat" not in location or "lon" not in location:
        raise AirflowException(f"Geolocation response is missing lat/lon: {location}")

    if not api_key:
        raise AirflowException("WEATHER_API_KEY Airflow variable is empty.")

    hook = HttpHook(method="GET", http_conn_id="weather_conn_http")
    response = hook.run(
        endpoint="data/3.0/onecall/timemachine",
        data={
            "appid": api_key,
            "lat": location["lat"],
            "lon": location["lon"],
            "dt": timestamp,
            "units": "metric",
        },
    )

    try:
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        raise AirflowException(f"Failed to fetch or parse weather data: {exc}") from exc

    if "data" not in payload or not payload["data"]:
        raise AirflowException(f"Weather response does not contain data: {payload}")

    return payload


def _process_weather(city, weather_data):
    if "data" not in weather_data or not weather_data["data"]:
        raise AirflowException(f"Weather data for {city} is empty or malformed: {weather_data}")

    measurement = weather_data["data"][0]
    required_fields = ["dt", "temp", "humidity", "clouds", "wind_speed"]
    missing_fields = [field for field in required_fields if field not in measurement]
    if missing_fields:
        raise AirflowException(
            f"Weather measurement for {city} is missing fields {missing_fields}: {measurement}"
        )

    record = {
        "city": city,
        "timestamp": measurement["dt"],
        "temperature": measurement["temp"],
        "humidity": measurement["humidity"],
        "cloudiness": measurement["clouds"],
        "wind_speed": measurement["wind_speed"],
    }
    logging.info(
        "City: %s, Timestamp: %s, Temperature: %s, Humidity: %s, Cloudiness: %s, Wind Speed: %s",
        record["city"],
        record["timestamp"],
        record["temperature"],
        record["humidity"],
        record["cloudiness"],
        record["wind_speed"],
    )
    return record


def _choose_load_path(record, group_id, wind_speed_threshold):
    if "wind_speed" not in record:
        raise AirflowException(f"Cannot branch without wind_speed in record: {record}")

    if record["wind_speed"] >= wind_speed_threshold:
        return f"{group_id}.alert"
    return f"{group_id}.load"


def _send_weather_alert(record, wind_speed_threshold):
    logging.warning(
        "Weather alert for %s: wind speed %s crossed threshold %s",
        record["city"],
        record["wind_speed"],
        wind_speed_threshold,
    )


def _load_weather(record):
    required_fields = ["city", "timestamp", "temperature", "humidity", "cloudiness", "wind_speed"]
    missing_fields = [field for field in required_fields if field not in record]
    if missing_fields:
        raise AirflowException(f"Cannot load record; missing fields {missing_fields}: {record}")

    try:
        hook = PostgresHook(postgres_conn_id="weather_conn")
        hook.run(
            """
            INSERT INTO measures (city, timestamp, temperature, humidity, cloudiness, wind_speed)
            VALUES (%s, to_timestamp(%s), %s, %s, %s, %s);
            """,
            parameters=(
                record["city"],
                record["timestamp"],
                record["temperature"],
                record["humidity"],
                record["cloudiness"],
                record["wind_speed"],
            ),
        )
    except Exception as exc:
        logging.exception("Failed to load weather record for %s", record["city"])
        raise AirflowException(f"Failed to load weather record for {record['city']}: {exc}") from exc


with DAG(
    dag_id="weather_processor",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
    catchup=True,
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id="create_table_postgres",
        conn_id="weather_conn",
        **DB_RETRY_ARGS,
        sql="""
        CREATE TABLE IF NOT EXISTS measures (
            city TEXT,
            timestamp TIMESTAMP,
            temperature FLOAT,
            humidity FLOAT,
            cloudiness FLOAT,
            wind_speed FLOAT
        );
        """,
    )

    for city in CITIES:
        city_slug = city.lower()
        group_id = f"{city_slug}_weather"

        with TaskGroup(group_id=group_id, tooltip=f"{city} extract-transform-load") as city_group:
            check_api = HttpSensor(
                task_id="check_api",
                http_conn_id="weather_conn_http",
                endpoint="geo/1.0/direct",
                request_params={
                    "appid": "{{ var.value.WEATHER_API_KEY }}",
                    "q": city,
                    "limit": 1,
                },
                poke_interval=30,
                timeout=300,
                mode="reschedule",
                **HTTP_RETRY_ARGS,
            )

            extract_geolocation = HttpOperator(
                task_id="extract_geolocation",
                http_conn_id="weather_conn_http",
                endpoint="geo/1.0/direct",
                data={
                    "appid": "{{ var.value.WEATHER_API_KEY }}",
                    "q": city,
                    "limit": 1,
                },
                method="GET",
                response_filter=lambda response: json.loads(response.text),
                log_response=True,
                **HTTP_RETRY_ARGS,
            )

            extract_data = PythonOperator(
                task_id="extract",
                python_callable=_fetch_weather,
                **HTTP_RETRY_ARGS,
                op_kwargs={
                    "geolocation": extract_geolocation.output,
                    "api_key": "{{ var.value.WEATHER_API_KEY }}",
                    "timestamp": "{{ data_interval_start.int_timestamp }}",
                },
            )

            process_data = PythonOperator(
                task_id="transform",
                python_callable=_process_weather,
                retries=1,
                retry_delay=timedelta(seconds=30),
                op_kwargs={
                    "city": city,
                    "weather_data": extract_data.output,
                },
            )

            choose_load_path = BranchPythonOperator(
                task_id="choose_load_path",
                python_callable=_choose_load_path,
                retries=1,
                retry_delay=timedelta(seconds=30),
                op_kwargs={
                    "record": process_data.output,
                    "group_id": group_id,
                    "wind_speed_threshold": WIND_SPEED_ALERT_THRESHOLD,
                },
            )

            load = PythonOperator(
                task_id="load",
                python_callable=_load_weather,
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                **DB_RETRY_ARGS,
                op_kwargs={"record": process_data.output},
            )

            alert = PythonOperator(
                task_id="alert",
                python_callable=_send_weather_alert,
                retries=2,
                retry_delay=timedelta(seconds=30),
                op_kwargs={
                    "record": process_data.output,
                    "wind_speed_threshold": WIND_SPEED_ALERT_THRESHOLD,
                },
            )

            check_api >> extract_geolocation >> extract_data >> process_data >> choose_load_path
            choose_load_path >> load
            choose_load_path >> alert >> load

        create_table >> city_group
