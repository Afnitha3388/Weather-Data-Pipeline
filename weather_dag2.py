from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone, timedelta
import requests
import psycopg2
import logging

API_KEY = 'my Api key'
CITIES = ["Frankfurt", "Berlin", "Hamburg", "Munich", "Cologne"]

def fetch_weather_data():
    try:
        # Connect to Postgres
        with psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow",
            port=5432
        ) as conn:
            with conn.cursor() as cur:
                # Create table if not exists
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS weather_data6 (
                        city TEXT,
                        date TIMESTAMP,
                        temperature FLOAT,
                        humidity INT,
                        pressure INT,
                        wind_speed FLOAT,
                        weather_description TEXT
                    )
                """)
                conn.commit()

                all_data = []

                for city in CITIES:
                    URL = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
                    response = requests.get(URL)
                    data = response.json()

                    timestamp = data.get("dt")
                    date = datetime.fromtimestamp(timestamp, tz=timezone.utc) if timestamp else None

                    weather_tuple = (
                        data.get("name"),
                        date,
                        data.get("main", {}).get("temp"),
                        data.get("main", {}).get("humidity"),
                        data.get("main", {}).get("pressure"),
                        data.get("wind", {}).get("speed"),
                        data.get("weather")[0].get("description") if data.get("weather") else None
                    )
                    all_data.append(weather_tuple)

                insert_query = """
                    INSERT INTO weather_data6 (
                        city, date, temperature, humidity, pressure, wind_speed, weather_description
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cur.executemany(insert_query, all_data)
                conn.commit()

                logging.info(f"Inserted {len(all_data)} rows into weather_data6.")

    except Exception as e:
        logging.error(f"Error fetching or inserting weather data: {e}")
        raise  # Raise error so Airflow marks task as failed

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_etl1',
    default_args=default_args,
    schedule='@hourly',
    catchup=False,
    tags=['example']
) as dag:

    fetch_weather_task = PythonOperator(
        task_id='fetch_weather_task',
        python_callable=fetch_weather_data
    )
