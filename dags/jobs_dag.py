"""
DAG: jobs_dag.py
Purpose:
- jobs_dag: Simulates a data pipeline with tasks to print DB info, conditionally insert/query/create table, and return a result.
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator, get_current_context
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


config = {
    'dag_id_1': {'schedule_interval': None, "start_date": datetime(2018, 11, 11), 'table_name': 'user_tracking_dag1', 'database': 'airflow'},
    'dag_id_2': {'schedule_interval': None, "start_date": datetime(2018, 11, 11), 'table_name': 'user_tracking_dag2', 'database': 'airflow'},
    'dag_id_3': {'schedule_interval': None, "start_date": datetime(2018, 11, 11), 'table_name': 'user_tracking_dag3', 'database': 'airflow'}
}


def print_info(dag_id, database, **context):
    print(f"{dag_id} start processing tables in database: {database}")
    print("*********************************************************")
    exec_date = datetime.fromisoformat(context['ts'])
    print(f"Execution date: {exec_date}, type: {type(exec_date)}")

def prepare_insert_values(**kwargs):
    from datetime import datetime
    import random

    user_name = kwargs['ti'].xcom_pull(task_ids='get_current_user')
    custom_id = random.randint(1000, 9999)
    timestamp = datetime.utcnow().isoformat()

    kwargs['ti'].xcom_push(key='insert_values', value={
        'custom_id': custom_id,
        'user_name': user_name,
        'timestamp': timestamp
    })

def insert_row(**kwargs):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    table = kwargs['table_name']
    values = kwargs['ti'].xcom_pull(task_ids='prepare_data', key='insert_values')

    sql = f"""
        INSERT INTO {table} (custom_id, user_name, timestamp)
        VALUES (%s, %s, %s);
    """

    hook = PostgresHook(postgres_conn_id='postgress_default')

    # Usa get_conn() y cursor manualmente
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql, (values['custom_id'], values['user_name'], values['timestamp']))
    conn.commit()
    cursor.close()
    conn.close()

def check_table_exists(**kwargs):
    table = kwargs['table_name']
    hook = PostgresHook(postgres_conn_id='postgress_default')
    query = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{table}'
        );
    """
    result = hook.get_first(query)
    return 'insert_new_row' if result and result[0] else 'create_table'

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
            op_kwargs={'dag_id': dag_id, 'database': params['database']},
        )

        get_user = BashOperator(
            task_id='get_current_user',
            bash_command='whoami',
            do_xcom_push=True
        )

        check = BranchPythonOperator(
            task_id='check_table',
            python_callable=check_table_exists,
            op_kwargs={'table_name': params['table_name']},
        )

        create = SQLExecuteQueryOperator(
            task_id='create_table',
            conn_id='postgress_default',
            sql=f"""
                CREATE TABLE IF NOT EXISTS {params['table_name']} (
                    custom_id INTEGER NOT NULL,
                    user_name VARCHAR(50) NOT NULL,
                    timestamp TIMESTAMP NOT NULL
                );
            """
        )

        prepare_data = PythonOperator(
            task_id='prepare_data',
            python_callable=prepare_insert_values
        )

        insert = PythonOperator(
            task_id='insert_new_row',
            python_callable=insert_row,
            op_kwargs={'table_name': params['table_name']},
        )

        query = PythonOperator(
            task_id='query_the_table',
            python_callable=push_message,
            op_kwargs={'dag_id': dag_id},
            do_xcom_push=True
        )

        print_info_task >> get_user >> check
        check >> create >> prepare_data >> insert
        check >> prepare_data >> insert
        insert >> query

    globals()[dag_id] = dag
