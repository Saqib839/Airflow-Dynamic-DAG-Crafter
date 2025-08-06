from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import mysql.connector
import importlib.util
import sys
import os
from datetime import timedelta

# Base path to the Github folder inside airflow directory
BASE_SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../Github'))

def dynamic_import(func_name, module_path):
    spec = importlib.util.spec_from_file_location("dynamic_module", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["dynamic_module"] = module
    spec.loader.exec_module(module)
    return getattr(module, func_name)

def get_schedule_interval(schedule_period, schedule_time):
    hour, minute, second = map(int, schedule_time.split(':'))
    if schedule_period == 'daily':
        return f'{minute} {hour} * * *'
    elif schedule_period == 'weekly':
        return f'{minute} {hour} * * 1'
    elif schedule_period == 'monthly':
        return f'{minute} {hour} 1 * *'
    elif schedule_period == 'yearly':
        return f'{minute} {hour} 1 1 *'
    else:
        return '@daily'

def fetch_metadata():
    conn = mysql.connector.connect(
        host='localhost',
        user='airflow',
        password='airflow',
        database='airflow_metadata'
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM pipeline_metadata")
    pipelines = cursor.fetchall()

    pipeline_task_map = {}
    for pipeline in pipelines:
        cursor.execute(f"SELECT * FROM task_metadata WHERE pipeline_id = {pipeline['pipeline_id']} ORDER BY task_order ASC")
        tasks = cursor.fetchall()
        pipeline_task_map[pipeline['pipeline_name']] = {
            'pipeline': pipeline,
            'tasks': tasks
        }
    conn.close()
    return pipeline_task_map

def create_dags():
    pipelines = fetch_metadata()

    for pipeline_name, metadata in pipelines.items():
        pipeline = metadata['pipeline']
        tasks = metadata['tasks']

        default_args = {
            'owner': 'airflow',
            'start_date': days_ago(1),
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        }

        schedule_interval = get_schedule_interval(pipeline['schedule_period'], str(pipeline['schedule_time']))

        dag = DAG(
            dag_id=pipeline_name,
            default_args=default_args,
            schedule_interval=schedule_interval,
            catchup=False,
            description=pipeline['description']
        )

        task_objects = {}

        for task_meta in tasks:
            task_params_raw = task_meta['task_params'].strip()
            params = task_params_raw.split()
            task_identifier = task_meta['task_identifier']

            # Compute absolute path to script
            if len(params) == 0:
                continue  # Skip if task_params is empty

            script_relative_path = params[0]
            script_path = os.path.abspath(os.path.join(BASE_SCRIPT_DIR, script_relative_path))

            if task_meta['task_type'] == 'BashOperator':
                args = ' '.join(params[1:])  # Remaining words as args
                bash_cmd = f"{script_path} {args}".strip()
                task = BashOperator(
                    task_id=task_identifier,
                    bash_command=bash_cmd,
                    dag=dag
                )

            elif task_meta['task_type'] == 'PythonOperator':
                if len(params) < 2:
                    raise ValueError(f"PythonOperator task '{task_identifier}' must provide script path and function name.")
                function_name = params[1]
                # Optional args can be handled here later if needed
                python_callable_func = dynamic_import(function_name, script_path)
                task = PythonOperator(
                    task_id=task_identifier,
                    python_callable=python_callable_func,
                    dag=dag
                )

            else:
                continue  # Unsupported type

            task_objects[task_identifier] = task

        # Apply Task Dependencies
        for task_meta in tasks:
            if task_meta['task_dependency']:
                downstream_task = task_objects[task_meta['task_identifier']]
                upstream_ids = [tid.strip() for tid in task_meta['task_dependency'].split(',')]
                for upstream_id in upstream_ids:
                    task_objects[upstream_id] >> downstream_task

        globals()[pipeline_name] = dag  # Register DAG to Airflow

create_dags()
