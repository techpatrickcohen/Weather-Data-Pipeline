import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Connection
from airflow.utils.db import provide_session
from airflow import settings
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import psycopg2
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


def sync_data():
    hook = PostgresHook(postgres_conn_id='airflow_db')
    engine = hook.get_sqlalchemy_engine()

    query = """
        SELECT city, latitude, longitude, timestamp, timezone, temperature, humidity, apparent_temperature, 
               rain, wind_direction, weather_code
        FROM weather_data
    """

    with engine.connect() as connection:
        local_data = connection.execute(query).fetchall()

    AWS_RDS_CONN = {
        'dbname': os.environ.get('AWS_RDS_CONN_DBNAME'),
        'user': os.environ.get('AWS_RDS_CONN_USER'),
        'password': os.environ.get('AWS_RDS_CONN_PASS'),
        'host': os.environ.get('AWS_RDS_CONN_HOST'),
        'port': 5432
    }

    # Connect to AWS RDS PostgreSQL
    rds_conn = psycopg2.connect(**AWS_RDS_CONN)
    rds_cursor = rds_conn.cursor()

    # Use MERGE to sync data in AWS RDS PostgreSQL
    merge_query = """
    MERGE INTO weather_data AS target
    USING (VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)) AS source (city, latitude, longitude, timestamp, timezone, temperature, humidity, 
              apparent_temperature, rain, wind_direction, weather_code)
    ON target.timestamp = source.timestamp AND target.city = source.city
    WHEN MATCHED THEN
        UPDATE SET latitude = source.latitude,
                   longitude = source.longitude,
                   timezone = source.timezone,
                   temperature = source.temperature,
                   humidity = source.humidity,
                   apparent_temperature = source.apparent_temperature,
                   rain = source.rain,
                   wind_direction = source.wind_direction,
                   weather_code = source.weather_code
    WHEN NOT MATCHED THEN
        INSERT (city, latitude, longitude, timestamp, timezone, temperature, humidity, 
                apparent_temperature, rain, wind_direction, weather_code)
        VALUES (source.city, source.latitude, source.longitude, source.timestamp, source.timezone, 
                source.temperature, source.humidity, source.apparent_temperature, 
                source.rain, source.wind_direction, source.weather_code);
    """

    # Execute MERGE operation for each row of data
    for row in local_data:
        rds_cursor.execute(merge_query, row)

    rds_conn.commit()

    rds_cursor.close()
    rds_conn.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sync_local_to_aws_rds',
    default_args=default_args,
    description='DAG to sync local PostgreSQL data to AWS RDS',
    # Sync every hour
    schedule_interval=timedelta(hours=1),
)

# Define DAG args
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1)
}

with DAG('sync_local_to_rds',
         default_args=default_args,
         # run every 15 min
         schedule_interval='*/15 * * * *',
         catchup=False,
         ) as dag:

    create_connection_task = PythonOperator(
        task_id='create_postgres_connection',
        python_callable=create_postgres_connection_if_not_exists
    )

    sync_task = PythonOperator(
        task_id='sync_data_to_rds',
        python_callable=sync_data
    )

    create_connection_task >> sync_task
