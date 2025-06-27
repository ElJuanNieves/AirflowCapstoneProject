from airflow import DAG

from datetime import datetime

from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import task_group
from sqlalchemy import false

default_args = {
    'start_date': datetime(2024, 1, 1),
}

def store_date(**context):
    execution_date = context['ti'].xcom_pull(
        task_id='trigger_target_dag', key='execution_date'
    )
    context['ti'].xcom_push(key='jobs_dag_date', value=execution_date)

def get_execution_date(**context):
    return context['ti'].xcom_pull(task_id='trigger_target_dag', key='jobs_dag_date')

with DAG(
    dag_id='trigger_dag',
    default_args=default_args,
    schedule = None,
    catchup = False
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_trigger_file',
        filepath= Variable.get("trigger_path"),
        poke_interval=10,
        timeout=300,
        mode='reschedule',
        fs_conn_id='fs_default'
    )

    @task_group(group_id="trigger_dag")
    def trigger_dag():

        trigger_target_dag = TriggerDagRunOperator(
            task_id='trigger_target_dag',
            trigger_dag_id='dag_id_1',
            wait_for_completion = False,
            execution_date='{{ ts }}',
            reset_on_failure = True
        )

        store_trigger_date = PythonOperator(
            task_id='store_trigger_date',
            python_callable=store_date,
            provide_context=True
        )

        wait_jobs_dag = ExternalTaskSensor(
            task_id='wait_jobs_dag',
            external_dag_id='dag_id_1',
            external_task_id=None,
            execution_date_fn=get_execution_date,
            timeout=300,
            poke_interval=10,
            mode='poke'
        )


        trigger_target_dag >> wait_jobs_dag >> trigger_target_dag

    @task_group(group_id="process_results")
    def process_results():


        remove_file = BashOperator(
            task_id='remove_trigger_file',
            bash_command='rm {{ var.value.trigger_path }}'
        )

    # Definir el orden
    wait_for_file >> trigger_target_dag >> remove_file
