import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from common.teams_message_operator import TeamsMessageOperator

DAG_ID = 'Webhook'

def throw_exception():
    statuss = 1
    if statuss == 1:
        print("Tudo certo, pode continuar")
    else:
        raise Exception('deu ruim')

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

    start_task = PythonOperator(
        task_id = 'start_task',
        python_callable = throw_exception
        )

    teste_task = PythonOperator(
        task_id = 'teste_task',
        python_callable = throw_exception
        )

    send_teams_message = TeamsMessageOperator(
        task_id = 'send_teams_message',
        mention_on_fail = ["CS383950"],
        hookurl = 'link do hook do teams'
        )
    
    start_task >> teste_task >> send_teams_message 