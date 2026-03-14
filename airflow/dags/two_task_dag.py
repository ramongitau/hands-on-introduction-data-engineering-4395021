from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'two_task_dag',
    default_args=default_args,
    schedule_interval=None,
    description='A simple two-task DAG',
    catchup=False,
)

t0 = BashOperator(
    task_id='T0',
    bash_command='echo "First Airflow task"',
    dag=dag,
)

t1 = BashOperator(
    task_id='T1',
    bash_command='sleep 5 && echo "Second Airflow task"',
    dag=dag,
)

t0 >> t1