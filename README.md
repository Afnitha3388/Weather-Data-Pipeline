# Weather-Data-Pipeline
An automated data pipeline that fetches live weather data from the OpenWeatherMap API for 5 major German cities every hour, and stores it into PostgreSQL — orchestrated with Apache Airflow running in Docker.

# Tech Stack

| Tool | Purpose |
|---|---|
| Python | Core pipeline logic |
| Apache Airflow 3.x | Pipeline orchestration |
| Docker | Containerized Airflow environment |
| Pandas | Data transformation |
| PostgreSQL | Data storage |
| SQLAlchemy | Database connection |
| Power BI | Data visualization |
| Kaggle API | Data source |

# Cities Tracked
City: Frankfurt, Berlin, Hamburg, Munich, Cologne

# Data Collected

| Column | Description |
|---|---|
| city | City name |
| date | Timestamp of the reading (UTC) |
| temperature | Temperature in Celsius |
| humidity | Humidity percentage |
| pressure | Atmospheric pressure (hPa) |
| wind_speed | Wind speed in m/s |
| weather_description | Weather condition description |

# Setup Instructions
Prerequisites

Docker Desktop installed and running
OpenWeatherMap API key — get one free at openweathermap.org

 1. Start Airflow with Docker
    docker compose up -d
2.Trigger the DAG
 Open Airflow UI at http://localhost:8080,
 Find weather_etl1
 The DAG runs automatically every hour
 Or click the play button to trigger manually

# DAG Details
with DAG(
    'weather_etl1',
    schedule='@hourly',     # runs every hour
    catchup=False,
    tags=['example']
) as dag:
    fetch_weather_task = PythonOperator(
        task_id='fetch_weather_task',
        python_callable=fetch_weather_data
    )
  
Fetches data from OpenWeatherMap API for each city
Creates the weather_data6 table automatically if it doesn't exist
Inserts all 5 city records per run
Retries once on failure with a 5 minute delay

