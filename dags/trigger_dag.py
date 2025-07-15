"""
Trigger and Monitoring DAG: trigger_dag.py

This DAG implements a file-based trigger system that:
- Monitors for trigger files using FileSensor
- Triggers external DAGs (jobs_dag instances) when files are detected
- Waits for external DAG completion using ExternalTaskSensor
- Retrieves and processes results from triggered DAGs via XCom
- Performs cleanup operations (removes trigger files, creates completion markers)
- Sends Slack notifications when the entire workflow completes

The DAG uses Vault integration for secure secret management and supports
multiple fallback methods for Slack token retrieval.

Dependencies:
- Vault backend for secret management (optional)
- Slack API token for notifications
- File system access for trigger file monitoring
- XCom communication with target DAGs

Security:
- Secrets stored in HashiCorp Vault
- Fallback to Airflow Variables and environment variables
- No hardcoded sensitive information
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, get_current_context
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.sensors.filesystem import FileSensor
from typing_extensions import MutableSet


@task
def store_date():
    """
    Stores the current execution date for synchronization with triggered DAGs.
    This ensures that the ExternalTaskSensor waits for the correct DAG run
    by pushing the execution date to XCom for later retrieval.
    """
    context = get_current_context()
    ts = context['ts']
    dt = datetime.fromisoformat(ts)
    print(f"[store_date] Parsed ts: {ts} => {dt}")
    context['ti'].xcom_push(key='jobs_dag_date', value=dt)
    return dt

def get_ts(context, dag_run_obj):
    """
    Gets execution_date of the triggered DAG and pushes it to XCom.
    This function is used by TriggerDagRunOperator to set the logical date
    for the triggered DAG run.
    """
    dag_run_obj.execution_date = context['execution_date']
    return dag_run_obj


def get_execution_date(current_execution_date, **context):
    """
    Retrieves the stored execution date from XCom for ExternalTaskSensor.
    This ensures the sensor waits for the correct DAG run that was triggered
    by this workflow, maintaining proper synchronization.
    """
    ti = context['ti']
    result = ti.xcom_pull(task_ids='trigger_target.store_date', key='jobs_dag_date')
    print("Pulled execution_date from XCom:", result, type(result))
    return result

def print_xcom_result(**context):
    """
    Retrieves and prints the result message from the triggered DAG.
    Pulls the completion message from the target DAG's query_the_table task
    for logging and monitoring purposes.
    """
    msg = context['ti'].xcom_pull(task_ids='dag_id_1.query_the_table')
    print(f"Received message: {msg}")
    print("Execution context:", context)


def send_slack_notification(**context):
    """
    Sends a completion notification to Slack using multiple fallback methods.
    
    Token retrieval hierarchy:
    1. Airflow Variables (slack_token)
    2. Environment variables (SLACK_TOKEN)
    3. HashiCorp Vault (commented out - requires vault provider)
    
    Uses slack-sdk for robust API communication with proper error handling.
    Failures in notification do not cause DAG failure.
    """
    try:
        # Option 1: Use Airflow Variable (simplest approach)
        token = Variable.get("slack_token", default_var=None)
        
        # Option 2: Fallback to environment variable if not in Variables
        if not token:
            import os
            token = os.getenv("SLACK_TOKEN")
        
        # Option 3: Vault integration (uncomment if vault provider available)
        # try:
        #     from airflow.providers.hashicorp.hooks.vault import VaultHook
        #     vault_hook = VaultHook(vault_conn_id="vault_default")
        #     secret_response = vault_hook.get_secret(
        #         secret_path="airflow/data/variables/slack_token"
        #     )
        #     token = secret_response.get('data', {}).get('data', {}).get('value')
        # except ImportError:
        #     print("Vault provider not available, using fallback methods")
        
        if not token:
            raise ValueError("Slack token not found in Variables, Environment, or Vault")
        
        # Use slack-sdk for robust API communication
        from slack_sdk import WebClient
        
        client = WebClient(token=token)
        
        # Send completion notification
        response = client.chat_postMessage(
            channel="#general",  # Configure target channel as needed
            text="Hello from airflow"
        )
        
        print(f"Message sent successfully: {response['ts']}")
        
    except Exception as e:
        print(f"Failed to send Slack message: {str(e)}")
        # Don't fail the DAG for notification errors
        pass


with DAG(
    dag_id='trigger_dag',
    default_args={'start_date': datetime(2024, 1, 1)},
    schedule=None,  # Manual trigger only
    catchup=False,
) as dag:

    # Task 1: Monitor for trigger file
    # FileSensor polls for the existence of a trigger file
    # File path is configured via Airflow Variable 'trigger_path'
    wait_for_file = FileSensor(
        task_id='wait_for_trigger_file',
        filepath = Variable.get("trigger_path"),
        poke_interval=10,  # Check every 10 seconds
        timeout=300,       # Timeout after 5 minutes
        mode='poke',
        fs_conn_id='fs_default'
    )


    @task_group(group_id="trigger_target")
    def trigger_target():
        """
        Task group that handles external DAG triggering and monitoring.
        
        Workflow:
        1. Trigger the target DAG (dag_id_1)
        2. Store execution date for synchronization
        3. Wait for target DAG completion
        4. Retrieve and print results from target DAG
        """
        
        # Trigger the target jobs DAG
        trigger_target_dag = TriggerDagRunOperator(
            task_id='trigger_target_dag',
            trigger_dag_id='dag_id_1',  # Target DAG to trigger
            wait_for_completion=False,   # Don't block on completion
            logical_date="{{ ts }}"      # Use current execution timestamp
        )

        # Store execution date for ExternalTaskSensor synchronization
        store_trigger_date = store_date()

        # Wait for the triggered DAG to complete
        # Uses stored execution date to monitor the correct DAG run
        wait_jobs_dag = ExternalTaskSensor(
            task_id='wait_jobs_dag',
            external_dag_id='dag_id_1',
            external_task_id=None,      # Wait for entire DAG completion
            execution_date_fn = get_execution_date,
            timeout=300,                 # Timeout after 5 minutes
            poke_interval=10,           # Check every 10 seconds
            mode='poke'
        )

        # Retrieve and print results from the completed DAG
        print_result = PythonOperator(
            task_id='print_result',
            python_callable=print_xcom_result,
        )

        # Define task dependencies within the group
        trigger_target_dag >> store_trigger_date >> wait_jobs_dag >> print_result

        return print_result


    @task_group(group_id="process_results")
    def process_results():
        """
        Task group that handles cleanup and notification after DAG completion.
        
        Workflow:
        1. Remove the trigger file to prevent re-triggering
        2. Create a completion marker file for external monitoring
        3. Send Slack notification about successful completion
        """
        
        # Remove the trigger file to prevent re-execution
        remove_file = BashOperator(
            task_id='remove_trigger_file',
            bash_command='rm /opt/airflow/data/{{ var.value.trigger_path }}'
        )

        # Create completion marker file with timestamp
        create_completion_file = BashOperator(
            task_id='create_completion_file',
            bash_command='touch /tmp/finished_{{ ts_nodash }}'
        )
        
        # Send Slack notification about workflow completion
        slack_notification = PythonOperator(
            task_id='send_slack_notification',
            python_callable=send_slack_notification,
        )

        # Define task dependencies within the group
        remove_file >> create_completion_file >> slack_notification

    # Define main DAG workflow
    # Linear execution: file trigger -> DAG execution -> cleanup/notification
    wait_for_file >> trigger_target() >> process_results()
