from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator

DAG_ID = 'Webhook'

default_args = {
    'owner': 'Webhook',
    'depends_on_past': False,
    "start_date": datetime(2023, 4, 15),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    'execution_timeout': timedelta(hours=2)
}

with DAG(
    dag_id = DAG_ID,
    default_args = default_args,
    max_active_runs = 1,
    schedule_interval= None, #agendamento
    catchup = False, #recuperadas dags anteriores
    doc_md = "DAG para teste de operador de webhook",
    tags=["Webhook"]
    ) as dag:

    start_task = DummyOperator(
        task_id = 'start_task')

    end_task = DummyOperator(
        task_id='end_task')

    start_task >> end_task
