import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Connection
from airflow.utils.db import provide_session
from airflow import settings
from datetime import datetime, timedelta
import pandas as pd
import requests
import logging


@provide_session
def create_postgres_connection_if_not_exists(session=None):
    """Check if the Postgres connection exists in Airflow, and if not, create it."""
    conn_id = 'airflow_db'

    # Check if the connection already exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

    if not existing_conn:
        # Create the connection if it doesn't exist
        new_conn = Connection(
            conn_id=conn_id,
            conn_type='postgres',
            host='postgres',
            schema='weather_db',
            login=os.environ.get('LOCAL_AIRFLOW_POSTGRESQL_USER'),
            password=os.environ.get('LOCAL_AIRFLOW_POSTGRESQL_PASS'),
            port=5432
        )
        session.add(new_conn)
        session.commit()
        logging.info(f"Created new Postgres connection with conn_id '{conn_id}'")
    else:
        logging.info(f"Postgres connection with conn_id '{conn_id}' already exists")


def get_city_coordinates(city_name):
    """Return the latitude and longitude for the city name."""
    coordinates = {
        "Miami": (25.7617, -80.1918),
        "New York": (40.7128, -74.0060),
        "London": (51.5074, -0.1278),
        "Tokyo": (35.6762, 139.6503),
    }
    return coordinates.get(city_name)


def check_if_historical_data_exists():
    """Check if there is any missing weather data for the past 5 days in the database."""
    hook = PostgresHook(postgres_conn_id='airflow_db')
    engine = hook.get_sqlalchemy_engine()

    # Define the past 5 days range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=5)

    query = """
    SELECT DATE(timestamp) as day, COUNT(*) as records
    FROM weather_data 
    WHERE timestamp >= %s AND timestamp < %s
    GROUP BY DATE(timestamp)
    ORDER BY day
    """

    with engine.connect() as connection:
        result = connection.execute(query, (start_date, end_date)).fetchall()

    # Convert the result into a set of days that have data
    days_with_data = {row['day'] for row in result}

    # Check if all days in the range are covered
    all_days = {start_date + timedelta(days=i) for i in range(5)}

    # If there is any day missing data, trigger the historical fetch task
    missing_days = all_days - days_with_data

    if missing_days:
        logging.warning(f"Data is missing for the following days: {missing_days}. Fetching historical data.")
        return 'fetch_historical_weather_data'
    else:
        logging.info("All days have data. Proceeding with fetching current weather data.")
        return 'fetch_current_weather_data'


def fetch_historical_weather_data(city):
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')

    # Step 1: Get existing timestamps from the database for the past 5 days
    hook = PostgresHook(postgres_conn_id='airflow_db')
    engine = hook.get_sqlalchemy_engine()

    query = """
    SELECT timestamp FROM weather_data 
    WHERE city = %s AND timestamp >= %s AND timestamp < %s
    """

    with engine.connect() as connection:
        result = connection.execute(query, (city.upper(), start_date, end_date)).fetchall()

    # Convert result to a set of existing timestamps
    existing_timestamps = {row['timestamp'].strftime('%Y-%m-%dT%H:00') for row in result}

    # Step 2: Fetch the historical weather data from the API
    lat, lon = get_city_coordinates(city)
    url = f"https://historical-forecast-api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}&hourly=temperature_2m,relative_humidity_2m,apparent_temperature,rain,weather_code,wind_direction_10m"

    response = requests.get(url)
    data_to_return = response.json()
    data_to_return['city'] = city.upper()

    if response.status_code != 200:
        logging.error(f"Failed to fetch historical data: {response.status_code}")
        raise Exception(f"Failed to fetch data: {response.status_code}")

    # Step 3: Filter out timestamps that already exist in the database
    hourly_data = data_to_return['hourly']
    missing_data = {
        'time': [],
        'temperature_2m': [],
        'relative_humidity_2m': [],
        'apparent_temperature': [],
        'rain': [],
        'weather_code': [],
        'wind_direction_10m': []
    }

    for i, timestamp in enumerate(hourly_data['time']):
        if timestamp not in existing_timestamps:
            # Add only missing data points
            missing_data['time'].append(timestamp)
            missing_data['temperature_2m'].append(hourly_data['temperature_2m'][i])
            missing_data['relative_humidity_2m'].append(hourly_data['relative_humidity_2m'][i])
            missing_data['apparent_temperature'].append(hourly_data['apparent_temperature'][i])
            missing_data['rain'].append(hourly_data['rain'][i])
            missing_data['weather_code'].append(hourly_data['weather_code'][i])
            missing_data['wind_direction_10m'].append(hourly_data['wind_direction_10m'][i])

    # Step 4: Return only the missing data
    logging.info(f"Fetched {len(missing_data['time'])} missing records for {city} from {start_date} to {end_date}")
    data_to_return['hourly'] = missing_data  # Replace the hourly data with missing data
    return data_to_return


def store_historical_weather_data(ti, **kwargs):
    historical_data = ti.xcom_pull(task_ids='fetch_historical_weather_data')
    if not historical_data:
        raise ValueError('No data received.')

    city = historical_data['city']
    timezone = historical_data['timezone']
    lat_data = round(historical_data['latitude'], 2)
    lon_data = round(historical_data['longitude'], 2)

    df_list = []
    hourly_data = historical_data['hourly']

    # Get the current timestamp to filter out data points from the current hour onwards
    current_time = datetime.now().strftime('%Y-%m-%dT%H:00')

    # Iterate through the historical data and append it only if the timestamp is earlier than the current time
    for i in range(len(hourly_data['time'])):
        if hourly_data['time'][i] < current_time:  # Trim data from the current hour onwards
            df_list.append({
                'city': city,
                'latitude': lat_data,
                'longitude': lon_data,
                'timestamp': hourly_data['time'][i],
                'timezone': timezone,
                'temperature': hourly_data['temperature_2m'][i],
                'humidity': hourly_data['relative_humidity_2m'][i],
                'apparent_temperature': hourly_data['apparent_temperature'][i],
                'rain': hourly_data['rain'][i],
                'wind_direction': hourly_data['wind_direction_10m'][i],
                'weather_code': hourly_data['weather_code'][i]
            })

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(df_list)

    hook = PostgresHook(postgres_conn_id='airflow_db')
    engine = hook.get_sqlalchemy_engine()

    logging.info(f"Here is the historical data frame that will be written to the db: {df}")
    df.to_sql('weather_data', engine, if_exists='append', index=False)


def fetch_current_weather_data(city, **kwargs):
    lat, lon = get_city_coordinates(city)
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,relative_humidity_2m,apparent_temperature,rain,weather_code,wind_direction_10m"

    response = requests.get(url)
    data_to_return = response.json()
    data_to_return['city'] = city.upper()

    if response.status_code == 200:
        logging.info(f"Weather data fetched successfully for {city}")
        return data_to_return
    else:
        logging.error(f"Failed to fetch data for {city}. Status code: {response.status_code}")
        raise Exception(f"Failed to fetch data: {response.status_code}")


def store_weather_data(ti, **kwargs):
    weather_data = ti.xcom_pull(task_ids='fetch_current_weather_data')
    if not weather_data:
        raise ValueError('No data received.')

    city = weather_data['city']
    timezone = weather_data['timezone']
    lat_data = round(weather_data['latitude'], 2)
    lon_data = round(weather_data['longitude'], 2)

    current_data = weather_data['current']

    weather_fields = {
        'temperature': current_data.get('temperature_2m'),
        'humidity': current_data.get('relative_humidity_2m'),
        'apparent_temperature': current_data.get('apparent_temperature'),
        'rain': current_data.get('rain'),
        'wind_direction': current_data.get('wind_direction_10m'),
        'weather_code': current_data.get('weather_code')
    }

    for field, value in weather_fields.items():
        if value is None:
            logging.warning(f"Missing {field.replace('_', ' ')} data for {city}")

    df = pd.DataFrame({
        'city': [city],
        'latitude': [lat_data],
        'longitude': [lon_data],
        'timestamp': [current_data['time']],
        'timezone': [timezone],
        'temperature': [weather_fields['temperature']],
        'humidity': [weather_fields['humidity']],
        'apparent_temperature': [weather_fields['apparent_temperature']],
        'rain': [weather_fields['rain']],
        'wind_direction': [weather_fields['wind_direction']],
        'weather_code': [weather_fields['weather_code']]
    })

    hook = PostgresHook(postgres_conn_id='airflow_db')
    engine = hook.get_sqlalchemy_engine()

    logging.info(f"Here is the data frame that will be written to the db: {df}")
    df.to_sql('weather_data', engine, if_exists='append', index=False)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'weather_data_pipeline',
        default_args=default_args,
        description='A DAG to fetch historical and current weather data',
        schedule_interval='*/15 * * * *',  # Every 15 minutes for current data
        catchup=False,
) as dag:

    create_connection_task = PythonOperator(
        task_id='create_postgres_connection',
        python_callable=create_postgres_connection_if_not_exists
    )

    check_historical_data_task_branch = BranchPythonOperator(
        task_id='check_historical_data',
        python_callable=check_if_historical_data_exists
    )

    fetch_historical_weather_data_task = PythonOperator(
        task_id='fetch_historical_weather_data',
        python_callable=fetch_historical_weather_data,
        op_kwargs={'city': 'Miami'},
        provide_context=True
    )

    store_historical_weather_data_task = PythonOperator(
        task_id='store_historical_weather_data',
        python_callable=store_historical_weather_data,
        provide_context=True
    )

    fetch_current_weather_data_task = PythonOperator(
        task_id='fetch_current_weather_data',
        python_callable=fetch_current_weather_data,
        op_kwargs={'city': 'Miami'},
        provide_context=True
    )

    store_current_weather_data_task = PythonOperator(
        task_id='store_current_weather_data',
        python_callable=store_weather_data,
        provide_context=True
    )

    create_weather_table = PostgresOperator(
        task_id="create_weather_table",
        postgres_conn_id="airflow_db",
        sql="""
        CREATE TABLE IF NOT EXISTS weather_data (
            city VARCHAR(50),
            latitude FLOAT,
            longitude FLOAT,
            timestamp TIMESTAMP,
            timezone VARCHAR(10),
            temperature FLOAT,
            humidity INTEGER,
            apparent_temperature FLOAT,
            rain FLOAT,
            wind_direction INTEGER,
            weather_code INTEGER
        );
        """
    )

    create_connection_task >> create_weather_table >> check_historical_data_task_branch
    check_historical_data_task_branch >> fetch_current_weather_data_task >> store_current_weather_data_task
    check_historical_data_task_branch >> fetch_historical_weather_data_task >> store_historical_weather_data_task
