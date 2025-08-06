---------------------------------------------Database & Tables Creation---------------------------------------------
-- Create Database
CREATE DATABASE airflow_metadata;
-- Use Database
USE airflow_metadata;

-- Create Tables
CREATE TABLE pipeline_metadata (
    pipeline_id INT AUTO_INCREMENT PRIMARY KEY,
    pipeline_name VARCHAR(255) NOT NULL,
    schedule_time TIME NOT NULL, 
    schedule_period ENUM('daily', 'weekly', 'monthly', 'yearly') NOT NULL,
    pipeline_dependency VARCHAR(255), -- Comma-separated list of pipeline_ids
    description TEXT
);

CREATE TABLE task_metadata (
    task_id INT AUTO_INCREMENT PRIMARY KEY,
    pipeline_id INT NOT NULL,
    task_identifier VARCHAR(255) NOT NULL, 
    task_type ENUM('BashOperator', 'PythonOperator') NOT NULL,
    task_params TEXT NOT NULL, 
    task_dependency VARCHAR(255), -- Comma-separated list of task_identifiers
    task_order INT NOT NULL,
    FOREIGN KEY (pipeline_id) REFERENCES pipeline_metadata(pipeline_id)
);


---------------------------------------------User Creation & Privileges---------------------------------------------
-- Create User
CREATE USER 'airflow'@'localhost' IDENTIFIED BY 'airflow';
-- Grant SELECT privileges only on airflow_metadata DB
GRANT SELECT ON airflow_metadata.* TO 'airflow'@'localhost';
-- Apply Changes
FLUSH PRIVILEGES;

---------------------------------------------Pipeline Metadata---------------------------------------------

-- Insert Pipelines
INSERT INTO pipeline_metadata (pipeline_id, pipeline_name, schedule_time, schedule_period, pipeline_dependency, description) VALUES
(1, 'etl_pipeline', '03:00:00', 'daily', NULL, 'ETL Pipeline Example'),
(2, 'validate_pipeline', '04:00:00', 'daily', NULL, 'Validation Pipeline Example');


---------------------------------------------Task Metadata---------------------------------------------

-- Tasks for Pipeline 1: etl_pipeline
INSERT INTO task_metadata (pipeline_id, task_identifier, task_type, task_params, task_dependency, task_order) VALUES
(1, 'extract_task', 'BashOperator', 'scripts/extract.sh --source db', NULL, 1),
(1, 'transform_task', 'PythonOperator', 'python_tasks/transform_task.py transform_data', 'extract_task', 2),
(1, 'load_task', 'BashOperator', 'scripts/load.sh --target warehouse', 'transform_task', 3),
(1, 'notify_task', 'PythonOperator', 'python_tasks/notify_task.py notify_completion', 'load_task', 4);

-- Tasks for Pipeline 2: validate_pipeline
INSERT INTO task_metadata (pipeline_id, task_identifier, task_type, task_params, task_dependency, task_order) VALUES
(2, 'extract_task_2', 'BashOperator', 'scripts/extract.sh --source api', NULL, 1),
(2, 'validate_task', 'PythonOperator', 'python_tasks/validate_task.py validate_data', 'extract_task_2', 2),
(2, 'cleanup_task', 'BashOperator', 'scripts/cleanup.sh', 'validate_task', 3);