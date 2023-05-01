from airflow.operators.python_operator import PythonOperator
from airflow.models import Connection
from airflow.models import TaskInstance
import urllib3
import typing

class TeamsMessageOperator(PythonOperator):
    
    def __init__(self, task_id: str, hookurl: str,
                executor_config: typing.Dict, payload: typing.Dict={}, 
                http_timeout: int=60, **context):
        self.hookurl = hookurl
        self.payload = payload
        self.http = urllib3.PoolManager()
        self.http_timeout = http_timeout

        super().__init__(
            task_id = task_id,
            executor_config = executor_config,
            python_callable=self.send_message_to_teams, 
            provide_context=True,
            trigger_rule='all_done')   

    def send_message_to_teams(self, **context):
        formatted_date = self.format_date(str(context['execution_date']))
        lista_task=[]
        dag_instance = context['dag']
        dag_id = dag_instance.dag_id        

        for task in dag_instance.tasks:
            task_status = TaskInstance(task, context['execution_date']).current_state()
            if task_status not in ['success', 'running', 'skipped']:
                lista_task.append(task.task_id)
        if len(lista_task) > 0:

            final_message = f"""                        
                        **DAG:** {(dag_id)}\n
                        **Tasks com falha:** {(lista_task)}\n
                        **Execution Time:** {(formatted_date)}\n
                        """           
            print(final_message)
        else:
            final_message = f"""
                        **DAG:** {(dag_id)}\n
                        **Nenhuma Task falhou!**\n
                        **Execution Time:** {(formatted_date)}\n
                        """
            print(final_message)