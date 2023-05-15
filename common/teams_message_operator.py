from airflow.operators.python_operator import PythonOperator
from airflow.models import Connection
from airflow.models import TaskInstance
import datetime
import urllib3
import json
import typing
import pytz


class TeamsMessageOperator(PythonOperator):
    
    def _init_(self, task_id: str, hookurl: str,
                executor_config: typing.Dict, payload: typing.Dict={}, 
                http_timeout: int=60, **context):
        self.hookurl = hookurl
        self.payload = payload
        self.http = urllib3.PoolManager()
        self.http_timeout = http_timeout

        super()._init_(
            task_id = task_id,
            executor_config = executor_config,
            python_callable=self.send_message_to_teams, 
            provide_context=True,
            trigger_rule='all_done')       

    def text(self, mtext: str):     
            self.payload["type"] = "message"
            self.payload["attachments"] = [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "contentUrl": None,
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [{"type": "TextBlock", "wrap": True, "text": mtext}],
                    "msteams": {"width": "Full"}
            }
        }]

    def send(self):
        headers = {"Content-Type":"application/json"}
        r = self.http.request(
                'POST',
                f'{self.hookurl}',
                body=json.dumps(self.payload).encode('utf-8'),
                headers=headers, timeout=self.http_timeout)
        if r.status == 200: 
            return True
        else:
            raise Exception(r.reason)

    def format_date(self, execution_date):
        utc_data = datetime.datetime.fromisoformat(execution_date)# Convertendo a string para um objeto datetime
        local_date = utc_data.astimezone(pytz.timezone('America/Sao_Paulo'))# Aplicando o fuso horário na data e hora
        formatted_date = local_date.strftime('%d-%m-%Y %H:%M')# Formatando a data e hora
        return formatted_date

    def send_message_to_teams(self, **context):
        formatted_date = self.format_date(str(context['execution_date']))
        lista_task=[]
        dag_instance = context['dag']
        dag_id = dag_instance.dag_id        

        for task in dag_instance.tasks:
            task_status = TaskInstance(task, context['execution_date']).current_state()
            if task_status not in ['success', 'running']:
                lista_task.append(task.task_id)
        print(len(lista_task))
        if len(lista_task) > 0:
            final_message = f"""
                        
                        *DAG:* {(dag_id)}\n

                        *Tasks com falha:* {(lista_task)}\n

                        *Execution Time:* {(formatted_date)}\n

                        """           
            self.text(mtext= final_message)
        else:
            final_message = f"""
                        *DAG:* {(dag_id)}\n

                        *Nenhuma Task falhou!*\n

                        *Execution Time:* {(formatted_date)}\n

                        """
            self.text(mtext= final_message)
            
        self.send()