from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

config = {
    'dag_id_1': {'schedule_interval': None, "start_date": datetime(2018, 11, 11), 'database': 'db1'},
    'dag_id_2': {'schedule_interval': None, "start_date": datetime(2018, 11, 11), 'database': 'db2'},
    'dag_id_3':{'schedule_interval': None, "start_date": datetime(2018, 11, 11), 'database': 'db3'},}

dags = {}

def print_info(dag_id, database, **kwargs):
    print(f"{dag_id} start processing tables in database: {database}")

def check_table_exist():
    if True:
        return 'insert_new_row'
    return 'create_table'


for dag_id, params in config.items():
    dag = DAG(
        dag_id,
        start_date = params["start_date"],
        schedule = params["schedule_interval"],
        catchup = False
    )
    # Tarea 1: imprimir información
    printInfo = PythonOperator(
        task_id='print_info',
        python_callable=print_info,
        op_kwargs={'dag_id': dag_id, 'database': params['database']},
        dag=dag
    )

    # Tarea 2: mock de inserción
    insert = EmptyOperator(
        task_id='insert_new_row',
        dag=dag,
        trigger_rule = 'none_failed'
    )

    # Tarea 3: mock de consulta
    query = EmptyOperator(
        task_id='query_the_table',
        dag=dag
    )

    check = BranchPythonOperator(
        task_id='check_table',
        python_callable=check_table_exist,
        dag=dag
    )

    create = EmptyOperator(
        task_id='create_table',
        dag=dag
    )

    getUser = BashOperator(
        task_id='get_current_user',
        bash_command='whoami',
        dag=dag
    )

    # Establecer dependencias
    printInfo >> getUser >> check
    check >> [create, insert]
    create >> insert
    insert >> query



    globals()[dag_id] = dag


