from datetime import datetime, timedelta
import pandas as pd
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
    'transform_dag',
    default_args=default_args,
    description='Transform TLD data using pandas',
    schedule_interval=None,
    catchup=False
)

def transform_data():
    # Get today's date
    today = datetime.today().date()
    
    # Read in our CSV
    df = pd.read_csv('/workspaces/hands-on-introduction-data-engineering-4395021/data/top-level-domain-names.csv')
    
    # Filter the dataframe down to only the rows with the type being generic
    df = df[df['Type'] == 'generic']
    
    # Append a new column named date with a nicely formatted version of today's date
    df['date'] = today.strftime('%Y-%m-%d')
    
    # Write out our new CSV
    df.to_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/airflow-transform-data.csv', index=False)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    dag=dag
)