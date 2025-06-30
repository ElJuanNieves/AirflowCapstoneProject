"""
DAG: jobs_dag.py
Purpose:
- jobs_dag: Simulates a data pipeline with tasks to print DB info, conditionally insert/query/create table, and return a result.
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

config = {
    'dag_id_1': {'schedule_interval': None, "start_date": datetime(2018, 11, 11), 'database': 'db1'},
    'dag_id_2': {'schedule_interval': None, "start_date": datetime(2018, 11, 11), 'database': 'db2'},
    'dag_id_3': {'schedule_interval': None, "start_date": datetime(2018, 11, 11), 'database': 'db3'}
}

def print_info(dag_id, database, **kwargs):
    print(f"{dag_id} start processing tables in database: {database}")

def check_table_exist():
    return 'insert_new_row' if True else 'create_table'

def push_message(**kwargs):
    return f"{kwargs['dag_id']} run_id={{ run_id }} ended"

for dag_id, params in config.items():
    with DAG(
        dag_id,
        start_date=params["start_date"],
        schedule=params["schedule_interval"],
        catchup=False,
    ) as dag:

        print_info_task = PythonOperator(
            task_id='print_info',
            python_callable=print_info,
            op_kwargs={'dag_id': dag_id, 'database': params['database']}
        )

        get_user = BashOperator(
            task_id='get_current_user',
            bash_command='whoami'
        )

        check = BranchPythonOperator(
            task_id='check_table',
            python_callable=check_table_exist
        )

        create = EmptyOperator(task_id='create_table')
        insert = EmptyOperator(task_id='insert_new_row', trigger_rule=TriggerRule.NONE_FAILED)

        query = PythonOperator(
            task_id='query_the_table',
            python_callable=push_message,
            op_kwargs={'dag_id': dag_id},
            do_xcom_push=True
        )

        print_info_task >> get_user >> check
        check >> [create, insert]
        create >> insert
        insert >> query

    globals()[dag_id] = dag
