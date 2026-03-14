from datetime import datetime, timedelta
import pandas as pd
import sqlite3
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='Complete ETL pipeline: Extract, Transform, Load TLD data',
    schedule_interval=None,
    catchup=False
)

def extract_data():
    # Read source data and save as extracted CSV
    df = pd.read_csv('/workspaces/hands-on-introduction-data-engineering-4395021/data/top-level-domain-names.csv')
    df.to_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/extracted-data.csv', index=False)
    print(f"Extracted {len(df)} records")

def transform_data():
    # Read extracted data, transform, and save
    df = pd.read_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/extracted-data.csv')
    df = df[df['Type'] == 'generic']
    df['date'] = datetime.today().date().strftime('%Y-%m-%d')
    df.to_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/transformed-data.csv', index=False)
    print(f"Transformed to {len(df)} generic TLD records")

def load_data():
    # Read transformed data and load into database
    df = pd.read_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/transformed-data.csv')
    df = df.rename(columns={
        'Sponsoring Organisation': 'SponsoringOrganization',
        'date': 'Date'
    })
    conn = sqlite3.connect('/workspaces/hands-on-introduction-data-engineering-4395021/lab/end-to-end/basic-etl-load-db.db')
    df.to_sql('top_level_domains', conn, if_exists='replace', index=False)
    conn.close()
    print(f"Loaded {len(df)} records into database")

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    dag=dag
)

# Set dependencies: extract -> transform -> load
extract_task >> transform_task >> load_task