"""
Airflow DAG for Bronze Layer ETL
Extracts data from MinIO and loads into PostgreSQL Bronze tables
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from pathlib import Path
import logging
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
# Import your ETL functions
from scripts.bronze.extractor import extract_from_minio
from scripts.bronze.loader import (
    create_bronze_tables,
    load_employees_to_bronze,
    load_timesheets_to_bronze,
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
    'bronze_layer_etl',
    default_args=default_args,
    description='Extract from MinIO and Load to PostgreSQL Bronze Layer',
    schedule='@daily',  # Run daily at 2 AM
    start_date=datetime(2025, 12, 17),
    catchup=False,
    tags=['bronze', 'etl', 'minio', 'postgres'],
)


# Task 1: Extract data from MinIO
def extract_task(**context):
    """Extract data from MinIO to local directory"""
    logger.info("Starting extraction from MinIO")
    
    download_dir = extract_from_minio(
        endpoint="minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket_name="rawdata",
        download_dir="datasets",
        secure=False,
    )
    
    # Push the directory path to XCom for next tasks
    context['ti'].xcom_push(key='data_dir', value=download_dir)
    logger.info(f"Extraction complete. Files saved to: {download_dir}")
    return download_dir


# Task 2: Create Bronze schema and tables
def create_tables_task(**context):
    """Create Bronze layer schema and tables if they don't exist"""
    logger.info("Creating Bronze layer tables")
    
    engine = get_engine()
    create_bronze_tables(engine)
    
    logger.info("Bronze tables created successfully")


# Task 3: Load employees to Bronze
def load_employees_task(**context):
    """Load employee CSV files into Bronze layer"""
    logger.info("Loading employees to Bronze layer")
    
    # Get data directory from previous task
    data_dir = context['ti'].xcom_pull(key='data_dir', task_ids='extract_from_minio')
    
    engine = get_engine()
    session = get_session()
    
    try:
        emp_count = load_employees_to_bronze(data_dir, session, engine)
        logger.info(f"Loaded {emp_count} employee records")
        
        # Push count to XCom
        context['ti'].xcom_push(key='employee_count', value=emp_count)
        return emp_count
    finally:
        session.close()


# Task 4: Load timesheets to Bronze
def load_timesheets_task(**context):
    """Load timesheet CSV files into Bronze layer"""
    logger.info("Loading timesheets to Bronze layer")
    
    # Get data directory from previous task
    data_dir = context['ti'].xcom_pull(key='data_dir', task_ids='extract_from_minio')
    
    engine = get_engine()
    session = get_session()
    
    try:
        ts_count = load_timesheets_to_bronze(data_dir, session, engine)
        logger.info(f"Loaded {ts_count} timesheet records")
        
        # Push count to XCom
        context['ti'].xcom_push(key='timesheet_count', value=ts_count)
        return ts_count
    finally:
        session.close()


# Task 5: Log summary
def log_summary_task(**context):
    """Log the ETL summary"""
    emp_count = context['ti'].xcom_pull(key='employee_count', task_ids='load_employees')
    ts_count = context['ti'].xcom_pull(key='timesheet_count', task_ids='load_timesheets')
    
    logger.info("=" * 60)
    logger.info("BRONZE LAYER ETL COMPLETE")
    logger.info(f"Employees loaded: {emp_count}")
    logger.info(f"Timesheets loaded: {ts_count}")
    logger.info("=" * 60)


# Define tasks
extract = PythonOperator(
    task_id='extract_from_minio',
    python_callable=extract_task,
    # provide_context=True,
    dag=dag,
)

create_tables = PythonOperator(
    task_id='create_bronze_tables',
    python_callable=create_tables_task,
    # provide_context=True,
    dag=dag,
)

load_employees = PythonOperator(
    task_id='load_employees',
    python_callable=load_employees_task,
    # provide_context=True,
    dag=dag,
)

load_timesheets = PythonOperator(
    task_id='load_timesheets',
    python_callable=load_timesheets_task,
    # provide_context=True,
    dag=dag,
)

log_summary = PythonOperator(
    task_id='log_summary',
    python_callable=log_summary_task,
    # provide_context=True,
    dag=dag,
)

# Define task dependencies
# Extract first, then create tables, then load data in parallel, then log summary
extract >> create_tables >> [load_employees, load_timesheets] >> log_summary