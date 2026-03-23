import json
import logging

import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta


CITIES = ["Lviv", "Kolomyia", "Kyiv", "Kharkiv", "Odesa"]

def _process_weather(city, ti):
    info = ti.xcom_pull(task_ids=f"extract_data_{city.lower()}")
    measurement = info["data"][0]
    timestamp = measurement["dt"]
    temperature = measurement["temp"]
    humidity = measurement["humidity"]
    cloudiness = measurement["clouds"]
    wind_speed = measurement["wind_speed"]
    logging.info(
        "City: %s, Timestamp: %s, Temperature: %s, Humidity: %s, Cloudiness: %s, Wind Speed: %s",
        city,
        timestamp,
        temperature,
        humidity,
        cloudiness,
        wind_speed,
    )
    return city, timestamp, temperature, humidity, cloudiness, wind_speed


with DAG(
    dag_id="weather_processor",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
    catchup=True,
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id="create_table_sqlite",
        conn_id="weather_conn",
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

        check_api = HttpSensor(
            task_id=f"check_api_{city_slug}",
            http_conn_id="weather_conn_http",
            endpoint="geo/1.0/direct",
            request_params={
                "appid": "{{ var.value.WEATHER_API_KEY }}",
                "q": city,
                "limit": 1,
            },
        )

        extract_geolocation = HttpOperator(
            task_id=f"extract_geolocation_{city_slug}",
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
        )

        extract_data = HttpOperator(
            task_id=f"extract_data_{city_slug}",
            http_conn_id="weather_conn_http",
            endpoint="data/3.0/onecall/timemachine",
            data={
                "appid": "{{ var.value.WEATHER_API_KEY }}",
                "lat": f"{{{{ ti.xcom_pull(task_ids='extract_geolocation_{city_slug}')[0]['lat'] }}}}",
                "lon": f"{{{{ ti.xcom_pull(task_ids='extract_geolocation_{city_slug}')[0]['lon'] }}}}",
                "dt": "{{ data_interval_start.int_timestamp }}",
                "units": "metric",
            },
            method="GET",
            response_filter=lambda response: json.loads(response.text),
            log_response=True,
        )

        process_data = PythonOperator(
            task_id=f"process_data_{city_slug}",
            python_callable=_process_weather,
            op_kwargs={"city": city},
        )

        inject_data = SQLExecuteQueryOperator(
            task_id=f"inject_data_{city_slug}",
            conn_id="weather_conn",
            retries=5,
            retry_delay=timedelta(seconds=5),
            sql=f"""
            INSERT INTO measures (city, timestamp, temperature, humidity, cloudiness, wind_speed)
            VALUES (
                '{{{{ ti.xcom_pull(task_ids="process_data_{city_slug}")[0] }}}}',
                {{{{ ti.xcom_pull(task_ids="process_data_{city_slug}")[1] }}}},
                {{{{ ti.xcom_pull(task_ids="process_data_{city_slug}")[2] }}}},
                {{{{ ti.xcom_pull(task_ids="process_data_{city_slug}")[3] }}}},
                {{{{ ti.xcom_pull(task_ids="process_data_{city_slug}")[4] }}}},
                {{{{ ti.xcom_pull(task_ids="process_data_{city_slug}")[5] }}}}
            );
            """,
        )

        create_table >> check_api >> extract_geolocation >> extract_data >> process_data >> inject_data
