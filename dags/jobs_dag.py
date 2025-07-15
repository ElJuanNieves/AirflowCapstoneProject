"""
Data Pipeline DAG: jobs_dag.py

This DAG simulates a data processing pipeline that performs the following operations:
- Prints database information and execution details
- Checks if a target table exists in PostgreSQL database
- Creates table if it doesn't exist or inserts data if it does
- Generates random data and inserts it into the database
- Returns a completion message via XCom

The DAG is dynamically created for multiple instances (dag_id_1, dag_id_2, dag_id_3)
each with its own table and configuration.

Dependencies:
- PostgreSQL database connection (postgress_default)
- Airflow standard operators
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator, get_current_context
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# Configuration for multiple DAG instances
# Each DAG will have its own table and database configuration
config = {
    'dag_id_1': {'schedule_interval': None, "start_date": datetime(2018, 11, 11), 'table_name': 'user_tracking_dag1', 'database': 'airflow'},
    'dag_id_2': {'schedule_interval': None, "start_date": datetime(2018, 11, 11), 'table_name': 'user_tracking_dag2', 'database': 'airflow'},
    'dag_id_3': {'schedule_interval': None, "start_date": datetime(2018, 11, 11), 'table_name': 'user_tracking_dag3', 'database': 'airflow'}
}


def print_info(dag_id, database, **context):
    """
    Prints DAG execution information including DAG ID, database, and execution date.
    This function provides logging for debugging and monitoring purposes.
    """
    print(f"{dag_id} start processing tables in database: {database}")
    print("*********************************************************")
    exec_date = datetime.fromisoformat(context['ts'])
    print(f"Execution date: {exec_date}, type: {type(exec_date)}")

def prepare_insert_values(**kwargs):
    """
    Prepares data values for database insertion.
    Generates random custom_id, retrieves username from previous task,
    and creates timestamp. Pushes prepared data to XCom for later use.
    """
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
    """
    Inserts prepared data into the specified PostgreSQL table.
    Retrieves data from XCom and executes INSERT SQL statement.
    Uses PostgreSQL hook to manage database connection.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    table = kwargs['table_name']
    values = kwargs['ti'].xcom_pull(task_ids='prepare_data', key='insert_values')

    sql = f"""
        INSERT INTO {table} (custom_id, user_name, timestamp)
        VALUES (%s, %s, %s);
    """

    hook = PostgresHook(postgres_conn_id='postgress_default')

    # Use get_conn() and cursor manually for direct database interaction
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql, (values['custom_id'], values['user_name'], values['timestamp']))
    conn.commit()
    cursor.close()
    conn.close()

def check_table_exists(**kwargs):
    """
    Checks if the target table exists in the PostgreSQL database.
    Returns task ID for branching: 'insert_new_row' if table exists,
    'create_table' if table doesn't exist.
    This enables conditional workflow execution.
    """
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
    """
    Creates a completion message for the DAG execution.
    This message is pushed to XCom and can be retrieved by other DAGs
    for monitoring and notification purposes.
    """
    return f"{kwargs['dag_id']} run_id={{ run_id }} ended"

# Dynamic DAG creation loop
# Creates multiple DAG instances based on the config dictionary
for dag_id, params in config.items():
    with DAG(
        dag_id,
        start_date=params["start_date"],
        schedule=params["schedule_interval"],
        catchup=False,
    ) as dag:

        # Task 1: Print execution information and database details
        print_info_task = PythonOperator(
            task_id='print_info',
            python_callable=print_info,
            op_kwargs={'dag_id': dag_id, 'database': params['database']},
        )

        # Task 2: Get current system user for data insertion
        get_user = BashOperator(
            task_id='get_current_user',
            bash_command='whoami',
            do_xcom_push=True
        )

        # Task 3: Branch based on table existence
        # Routes to either table creation or direct data insertion
        check = BranchPythonOperator(
            task_id='check_table',
            python_callable=check_table_exists,
            op_kwargs={'table_name': params['table_name']},
        )

        # Task 4: Create table if it doesn't exist
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

        # Task 5: Prepare data for insertion
        # Generates random data and formats it for database insertion
        prepare_data = PythonOperator(
            task_id='prepare_data',
            python_callable=prepare_insert_values
        )

        # Task 6: Insert data into the table
        insert = PythonOperator(
            task_id='insert_new_row',
            python_callable=insert_row,
            op_kwargs={'table_name': params['table_name']},
        )

        # Task 7: Generate completion message
        # Pushes final message to XCom for external consumption
        query = PythonOperator(
            task_id='query_the_table',
            python_callable=push_message,
            op_kwargs={'dag_id': dag_id},
            do_xcom_push=True
        )

        # Define task dependencies
        # Linear flow with conditional branching for table creation
        print_info_task >> get_user >> check
        check >> create >> prepare_data >> insert  # Path when table doesn't exist
        check >> prepare_data >> insert             # Path when table exists
        insert >> query

    # Register DAG in global namespace for Airflow discovery
    globals()[dag_id] = dag
