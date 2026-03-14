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
    'load_dag',
    default_args=default_args,
    description='Load transformed TLD data into SQLite database',
    schedule_interval=None,
    catchup=False
)

def load_data():
    # Read the transformed CSV
    df = pd.read_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/airflow-transform-data.csv')
    
    # Rename columns to match database schema
    df = df.rename(columns={
        'Sponsoring Organisation': 'SponsoringOrganization',
        'date': 'Date'
    })
    
    # Connect to the database
    conn = sqlite3.connect('/workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/airflow-load-db.db')
    
    # Insert data into the table
    df.to_sql('top_level_domains', conn, if_exists='append', index=False)
    
    # Close connection
    conn.close()
    
    print(f"Loaded {len(df)} records into the database")

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    dag=dag
)