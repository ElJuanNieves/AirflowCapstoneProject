"""
DAG: trigger_dag.py
Purpose: Watches for a trigger file, triggers a target DAG (jobs DAG), waits for its success,
         prints XCom result, and manages cleanup (removes trigger file and creates completion file).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, get_current_context
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.sensors.filesystem import FileSensorAsync
from typing_extensions import MutableSet


@task
def store_date():
    context = get_current_context()
    ts = context['ts']
    dt = datetime.fromisoformat(ts)
    print(f"[store_date] Parsed ts: {ts} => {dt}")
    context['ti'].xcom_push(key='jobs_dag_date', value=dt)
    return dt

def get_ts(context, dag_run_obj):
    """Gets execution_date of the triggered DAG and pushes it to XCom."""
    dag_run_obj.execution_date = context['execution_date']
    return dag_run_obj


def get_execution_date(current_execution_date, **context):
    ti = context['ti']
    result = ti.xcom_pull(task_ids='trigger_target.store_date', key='jobs_dag_date')
    print("Pulled execution_date from XCom:", result, type(result))
    return result

def print_xcom_result(**context):
    """Prints XCom message and the execution context."""
    msg = context['ti'].xcom_pull(task_ids='dag_id_1.query_the_table')
    print(f"Received message: {msg}")
    print("Execution context:", context)


def send_slack_notification(**context):
    """Sends a notification to Slack when DAG completes."""
    try:
        # Opción 1: Usar Variable de Airflow (más simple)
        token = Variable.get("slack_token", default_var=None)
        
        if not token:
            raise ValueError("Slack token not found in Variables, Environment, or Vault")
        
        from slack_sdk import WebClient
        
        client = WebClient(token=token)
        
        # Send message
        response = client.chat_postMessage(
            channel="#general",  # Puedes cambiar el canal aquí
            text="Hello from airflow"
        )
        
        print(f"Message sent successfully: {response['ts']}")
        
    except Exception as e:
        print(f"Failed to send Slack message: {str(e)}")
        # No queremos que falle el DAG por un error de notificación
        pass


with DAG(
    dag_id='trigger_dag',
    default_args={'start_date': datetime(2024, 1, 1)},
    schedule=None,
    catchup=False,
) as dag:

    wait_for_file = FileSensorAsync(
        task_id='wait_for_trigger_file',
        filepath = Variable.get("trigger_path"),
        poke_interval=10,
        timeout=300,
        mode='poke',
        fs_conn_id='fs_default'
    )


    @task_group(group_id="trigger_target")
    def trigger_target():
        trigger_target_dag = TriggerDagRunOperator(
            task_id='trigger_target_dag',
            trigger_dag_id='dag_id_1',
            wait_for_completion=False,
            logical_date="{{ ts }}"
        )

        store_trigger_date = store_date()

        wait_jobs_dag = ExternalTaskSensor(
            task_id='wait_jobs_dag',
            external_dag_id='dag_id_1',
            external_task_id=None,
            execution_date_fn = get_execution_date,
            timeout=300,
            poke_interval=10,
            mode='poke'
        )

        print_result = PythonOperator(
            task_id='print_result',
            python_callable=print_xcom_result,
        )

        trigger_target_dag >> store_trigger_date >> wait_jobs_dag >> print_result

        return print_result


    @task_group(group_id="process_results")
    def process_results():
        remove_file = BashOperator(
            task_id='remove_trigger_file',
            bash_command='rm /opt/airflow/data/{{ var.value.trigger_path }}'
        )

        create_completion_file = BashOperator(
            task_id='create_completion_file',
            bash_command='touch /tmp/finished_{{ ts_nodash }}'
        )
        
        slack_notification = PythonOperator(
            task_id='send_slack_notification',
            python_callable=send_slack_notification,
        )

        remove_file >> create_completion_file >> slack_notification

    wait_for_file >> trigger_target() >> process_results()
