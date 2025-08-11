from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import mysql.connector
import importlib.util
import sys
import os
from datetime import timedelta
from typing import Dict, Any

def load_config() -> Dict[str, Any]:
    """Load configuration from config.yml file"""
    config_path = os.path.join(os.path.dirname(__file__), "config.yml")
    try:
        import yaml
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
            return config
    except ImportError:
        print("Warning: PyYAML not installed. Using default configuration.")
        return {}
    except Exception as e:
        print(f"Warning: Could not load config.yml: {e}")
        return {}

# Load configuration
CONFIG = load_config()

# Calculate paths once
script_dir = CONFIG["PATHS"]["BASE_SCRIPT_DIR"]
if os.path.isabs(script_dir):
    BASE_SCRIPT_DIR = script_dir
else:
    BASE_SCRIPT_DIR = os.path.abspath(
        os.path.join(os.path.dirname(__file__), script_dir)
    )

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
        host=CONFIG["MYSQL_HOST"],
        port=CONFIG["MYSQL_PORT"],
        user=CONFIG["MYSQL_USER"],
        password=CONFIG["MYSQL_PASSWORD"],
        database=CONFIG["MYSQL_DB"],
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {CONFIG['PIPELINE_TABLE']}")
    pipelines = cursor.fetchall()

    pipeline_task_map = {}
    for pipeline in pipelines:
        cursor.execute(
            f"SELECT * FROM {CONFIG['TASK_TABLE']} WHERE pipeline_id = {pipeline['pipeline_id']} ORDER BY task_order ASC"
        )
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
            'owner': CONFIG["DEFAULT_OWNER"],
            'start_date': days_ago(CONFIG["DEFAULT_START_DATE_DAYS_AGO"]),
            'retries': CONFIG["DEFAULT_RETRIES"],
            'retry_delay': timedelta(minutes=CONFIG["DEFAULT_RETRY_DELAY_MINUTES"]),
        }

        schedule_interval = get_schedule_interval(pipeline['schedule_period'], str(pipeline['schedule_time']))

        dag = DAG(
            dag_id=pipeline_name,
            default_args=default_args,
            schedule_interval=schedule_interval,
            catchup=CONFIG["CATCHUP"],
            description=pipeline.get('description') or CONFIG["DAG_DESCRIPTION_FALLBACK"],
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
