from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# ----------------------
# Default Arguments
# ----------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['alerts@company.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# ----------------------
# Python Task Functions
# ----------------------
def start_task_func():
    print("Starting the DAG execution...")

def process_data_func():
    print("Processing data in Python Task...")

def final_step_func():
    print("All tasks are done. Finalizing DAG run...")

# ----------------------
# DAG Definition
# ----------------------
with DAG(
    dag_id='multi_task_dependency_dag',
    default_args=default_args,
    description='DAG with multiple tasks demonstrating fan-out and fan-in',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example', 'multi-task'],
) as dag:

    # ----------------------
    # Task Definitions
    # ----------------------

    # Start Task (PythonOperator)
    start_task = PythonOperator(
        task_id='start_task',
        python_callable=start_task_func
    )

    # Parallel Bash Tasks (Fan-out)
    bash_task_1 = BashOperator(
        task_id='bash_task_1',
        bash_command='echo "Running Bash Task 1: $(date)"'
    )

    bash_task_2 = BashOperator(
        task_id='bash_task_2',
        bash_command='echo "Running Bash Task 2: $(date)"'
    )

    # Python Data Processing Task
    python_processing_task = PythonOperator(
        task_id='python_processing_task',
        python_callable=process_data_func
    )

    # Join Task (Waits for All)
    join_task = BashOperator(
        task_id='join_task',
        bash_command='echo "Joining tasks before final step"'
    )

    # Final Task
    final_task = PythonOperator(
        task_id='final_step_task',
        python_callable=final_step_func
    )

    # ----------------------
    # Task Dependencies
    # ----------------------

    # Start Task triggers bash_task_1, bash_task_2, and python_processing_task in parallel
    start_task >> [bash_task_1, bash_task_2, python_processing_task]

    # Join Task waits for all three parallel tasks to finish
    [bash_task_1, bash_task_2, python_processing_task] >> join_task

    # Final Task runs after Join Task
    join_task >> final_task
