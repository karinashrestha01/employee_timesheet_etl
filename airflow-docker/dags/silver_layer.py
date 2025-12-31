"""
Airflow DAG for Silver Layer ETL
Transforms and cleans data from Bronze layer and loads into PostgreSQL Staging tables
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState
from pathlib import Path
import logging
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import your Silver ETL functions
from scripts.silver.transformer import (
    create_silver_tables,
    run_silver_transform,
)
from db.db_utils import get_engine, get_session

logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'silver_layer_etl',
    default_args=default_args,
    description='Transform and Clean data from Bronze to Silver Staging Layer',
    schedule='@daily',  # Run daily after Bronze layer
    start_date=datetime(2025, 12, 24),
    catchup=False,
    tags=['silver', 'etl', 'staging', 'transformation'],
)

# Sensor to wait for Bronze layer DAG to complete
wait_for_bronze = ExternalTaskSensor(
    task_id='wait_for_bronze_layer',
    external_dag_id='bronze_layer_etl',
    external_task_id=None,  # Wait for entire DAG to complete
    allowed_states=[DagRunState.SUCCESS], 
    failed_states=[DagRunState.FAILED],     
    mode='reschedule',
    timeout=3600,  # 1 hour timeout, If Bronze isn't done in 1 hour, fail the Silver task
    poke_interval=300,  # Check every 5 minutes
    dag=dag,
)

# Task 1: Create Silver schema and staging tables
def create_staging_tables_task(**context):
    """Create Silver layer schema and staging tables if they don't exist"""
    logger.info("Creating Silver layer staging tables")
    
    engine = get_engine()
    create_silver_tables(engine)
    
    logger.info("Silver staging tables created successfully")


create_tables = PythonOperator(
    task_id='create_staging_tables',
    python_callable=create_staging_tables_task,
    dag=dag,
)

# Task 2: Run incremental Silver transformation
def transform_silver_data_task(**context):
    """
    Run incremental Silver layer transformation:
    - Load new records from Bronze layer using watermarks
    - Clean and transform employee data
    - Clean and transform timesheet data
    - Run validation checks
    - Load into staging tables
    """
    logger.info("Starting Silver layer transformation")
    
    # Run the Silver transformation with validation
    result = run_silver_transform(validate=True)
    
    # Log results
    logger.info(f"Silver transformation complete:")
    logger.info(f"  Batch ID: {result['batch_id']}")
    logger.info(f"  Employees processed: {result['employees']}")
    logger.info(f"  Timesheets processed: {result['timesheets']}")
    
    # Push results to XCom
    context['ti'].xcom_push(key='batch_id', value=result['batch_id'])
    context['ti'].xcom_push(key='employee_count', value=result['employees'])
    context['ti'].xcom_push(key='timesheet_count', value=result['timesheets'])
    context['ti'].xcom_push(key='validation_passed', value=all(
        report.passed for report in result['validation']
    ))
    
    # Check if validation passed
    validation_failed = any(
        not report.passed for report in result['validation']
    )
    
    if validation_failed:
        logger.error("Silver layer validation failed!")
        raise ValueError("Silver layer validation checks failed")
    
    logger.info("Silver layer transformation and validation successful")
    return result


transform_silver = PythonOperator(
    task_id='transform_silver_data',
    python_callable=transform_silver_data_task,
    dag=dag,
)


# Task 3: Generate transformation summary
def generate_summary_task(**context):
    """Generate summary report of the Silver layer transformation"""
    logger.info("Generating Silver layer transformation summary")
    
    # Pull data from previous task
    batch_id = context['ti'].xcom_pull(key='batch_id', task_ids='transform_silver_data')
    emp_count = context['ti'].xcom_pull(key='employee_count', task_ids='transform_silver_data')
    ts_count = context['ti'].xcom_pull(key='timesheet_count', task_ids='transform_silver_data')
    validation_passed = context['ti'].xcom_pull(key='validation_passed', task_ids='transform_silver_data')
    
    logical_date = context.get('logical_date') or context.get('execution_date') or datetime.utcnow()
    
    summary = {
        'dag_run_date': logical_date.isoformat(),  
        'batch_id': batch_id,
        'records_processed': {
            'employees': emp_count,
            'timesheets': ts_count,
            'total': emp_count + ts_count
        },
        'validation_status': 'PASSED' if validation_passed else 'FAILED',
        'layer': 'silver',
    }
    logger.info("=" * 60)
    logger.info("SILVER LAYER ETL SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Execution Date: {summary['dag_run_date']}")
    logger.info(f"Batch ID: {summary['batch_id']}")
    logger.info(f"Employees Processed: {summary['records_processed']['employees']}")
    logger.info(f"Timesheets Processed: {summary['records_processed']['timesheets']}")
    logger.info(f"Total Records: {summary['records_processed']['total']}")
    logger.info(f"Validation: {summary['validation_status']}")
    logger.info("=" * 60)
    
    context['ti'].xcom_push(key='summary', value=summary)
    return summary


generate_summary = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary_task,
    dag=dag,
)


# Define task dependencies
wait_for_bronze >> create_tables >> transform_silver >> generate_summary