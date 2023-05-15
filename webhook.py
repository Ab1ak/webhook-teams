import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

sys.path.insert(0,os.path.abspath(os.path.dirname(_file_)))

from common.teams_message_operator import TeamsMessageOperator

DAG_ID = 'Webhook'

EXECUTOR_CONFIG= {
    'KubernetesExecutor' : { 
        'request_cpu' : '2',
        'request_memory' : '2Gi',
        'namespace' : " "
    }
}

default_args = {
    'owner': 'Webhook',
    'depends_on_past': False,
    "start_date": datetime(2023, 3, 7),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    'execution_timeout': timedelta(hours=2)
}

def throw_exception():
    statuss = 2
    if statuss == 1:
        print("Tudo certo, pode continuar")
    else:
        raise Exception('deu ruim')

with DAG(
    dag_id = DAG_ID,
    default_args = default_args,
    max_active_runs = 1,
    schedule_interval= None,
    catchup = False,
    doc_md = "DAG somente para teste de operador de webhook",
    tags=["teams", "Webhook"]
    ) as dag:

    start_task = PythonOperator(
        task_id = 'start_task',
        python_callable = throw_exception,
        executor_config=EXECUTOR_CONFIG
        )

    teste_task = PythonOperator(
        task_id = 'teste_task',
        python_callable = throw_exception,
        executor_config=EXECUTOR_CONFIG
        )

    send_teams_message = TeamsMessageOperator(
        task_id = 'send_teams_message',
        hookurl = 'adicionar aqui o link do webhook', #adicionar aqui o link do webhook
        executor_config=EXECUTOR_CONFIG
        )


    start_task >> teste_task >> send_teams_message