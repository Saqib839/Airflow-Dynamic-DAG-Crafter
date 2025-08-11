# Airflow-Dynamic-DAG-Crafter
This project enables dynamic DAG creation in Apache Airflow by reading pipeline and task definitions from MySQL metadata tables.
No need to create static DAG files for each workflow. You can control:
  -  Pipelines (DAGs) → via pipeline_metadata table
  -  Tasks (Bash/Python) → via task_metadata table
Airflow dynamically generates and updates DAGs based on the database metadata.

## Features
- No static DAG files required per workflow.
- Define and manage pipelines & tasks directly from MySQL tables.
- Supports BashOperator and PythonOperator.
- Fully supports task dependencies (series & parallel).
- Executes scripts from a local Github directory inside Airflow.

# Setup Instructions

## Step.1 --- Install Apache Airflow
Follow the official installation guide: https://airflow.apache.org/docs/apache-airflow/stable/start.html

## Step.2 --- Install MySQL
Install mysql server and create Database, Tables, and User.
Follow testing_resources/database.sql

## Step.3 --- Configure the DAG Factory
Create the configuration file:
```
    ~/airflow/dags/config.yml
```
This file contains all configurable settings for the DAG factory including:
- MySQL connection details
- Database table names
- Default DAG arguments
- Script directory paths
- DAG behavior settings

## Step.4 --- Place dag_factory.py in Airflow DAGs Folder
```
~/airflow/dags/dag_factory.py
```
This file dynamically generates DAGs from MySQL metadata using the configuration from `config.yml`.

## Step.5 --- Start Airflow Services
```
airflow scheduler
airflow webserver --port 8080
```
Access Airflow UI at: http://localhost:8080


------------------------------------------------------------------------------------------------------------

# Configuration

## dags/config.yml
The configuration file contains all settings for the DAG factory:
```yaml
# Path configuration
PATHS:
  BASE_SCRIPT_DIR: ../Github  # Script directory path

# MySQL connection configuration
MYSQL_HOST: localhost
MYSQL_PORT: 3306
MYSQL_USER: airflow
MYSQL_PASSWORD: airflow
MYSQL_DB: airflow_metadata

# Database table names
PIPELINE_TABLE: pipeline_metadata
TASK_TABLE: task_metadata

# Default DAG arguments
DEFAULT_OWNER: airflow
DEFAULT_START_DATE_DAYS_AGO: 1
DEFAULT_RETRIES: 1
DEFAULT_RETRY_DELAY_MINUTES: 5

# DAG behavior settings
CATCHUP: false
DAG_DESCRIPTION_FALLBACK: ""
```

## dags/dag_factory.py
The dag_factory.py file is the engine that dynamically creates Airflow DAGs by reading pipeline and task metadata from the MySQL database.
Key Responsibilities
- Connects to MySQL to fetch pipeline and task metadata.
- Dynamically creates Airflow DAGs based on pipeline configurations.
- Adds BashOperator and PythonOperator tasks from metadata.
- Parses task parameters (script path & arguments) from simple text.
- Automatically sets task dependencies (series/parallel) as per metadata.
- Registers DAGs in Airflow without needing static Python DAG files.
- Executes scripts from the Github/ directory inside Airflow project.

------------------------------------------------------------------------------------------------------------

# Metadata Tables Schema
- pipeline_metadata
```
| Column              | Description                                 |
|---------------------|---------------------------------------------|
| pipeline_id          | Unique ID for the pipeline (Primary Key)   |
| pipeline_name        | Name of the DAG (Unique DAG ID in Airflow) |
| schedule_time        | Time of day (HH:MM:SS) for DAG to trigger  |
| schedule_period      | Frequency: daily, weekly, monthly, yearly  |
| pipeline_dependency  | (Optional) Dependency on other pipelines   |
| description          | DAG description (visible in Airflow UI)    |
```
- task_metadata
```
| Column              | Description                                |
|---------------------|--------------------------------------------|
| task_id              | Unique ID for the task (Primary Key)      |
| pipeline_id          | Foreign key to pipeline_metadata          |
| task_identifier      | Unique Airflow task_id within the DAG     |
| task_type            | Operator Type: BashOperator or PythonOperator |
| task_params          | Simple text: `script_path args_if_any`   |
| task_dependency      | Comma-separated list of dependent tasks   |
| task_order           | Numeric value to control task execution order |
```

------------------------------------------------------------------------------------------------------------

# testing_resources/airflow-scheduler.service, airflow-webserver.service, enable_services.sh 
testing_resources/
 ├── airflow-scheduler.service    # systemd service file for Airflow Scheduler
 ├── airflow-webserver.service    # systemd service file for Airflow Webserver
 └── enable_services.sh           # Shell script to install and enable these services

- airflow-scheduler.service
```
  systemd service configuration for Airflow Scheduler.
  Keeps the Scheduler running in the background.
  Restarts automatically if it fails.
  Starts automatically on system boot.
```

- airflow-webserver.service
```
  systemd service configuration for Airflow Webserver.
  Exposes Airflow UI on port 8080.
  Restarts automatically on failure.
  Starts on system boot.
```
- enable_services.sh
```
  A helper script to:
  Copy both service files to /etc/systemd/system/.
  Reload systemd daemon.
  Enable both services to auto-start on boot.
  Start both services immediately.
```

------------------------------------------------------------------------------------------------------------
