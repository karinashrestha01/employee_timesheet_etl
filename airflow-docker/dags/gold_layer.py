"""
Airflow DAG for Gold Layer ETL
Transforms Silver staging data into dimensional model (dimensions and facts).
"""

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Add project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import Gold layer functions
from scripts.gold.loader import (
    create_gold_tables,
    load_staging_employees,
    load_staging_timesheets,
    transform_dim_department,
    transform_dim_employee,
    transform_dim_date,
    transform_fact_timesheet,
    insert_new_dates,
)
from scripts.gold.refresh import refresh_fact_timesheet
from db.db_utils import get_engine, get_session, upsert_dataframe
from db.models import DimDepartment, DimEmployee, DimDate, FactTimesheet

import logging

logger = logging.getLogger(__name__)


# Default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# Task functions
def create_tables_task():
    """Create Gold layer tables if they don't exist."""
    logger.info("Creating Gold layer tables...")
    engine = get_engine()
    create_gold_tables(engine)
    logger.info("Gold tables creation complete")


def load_and_transform_dimensions_task(**context):
    """Load staging data and transform dimension tables."""
    logger.info("Loading staging data and transforming dimensions...")
    
    engine = get_engine()
    
    # Load staging data
    stg_emp_df = load_staging_employees(engine)
    stg_ts_df = load_staging_timesheets(engine)
    
    if stg_emp_df.empty:
        logger.warning("No employee data in staging - skipping")
        context['ti'].xcom_push(key='skip_load', value=True)
        return
    
    # Transform dimensions
    df_dept = transform_dim_department(stg_emp_df)
    df_emp = transform_dim_employee(stg_emp_df, df_dept)
    df_date = transform_dim_date(stg_ts_df)
    
    # Push to XCom for next tasks
    context['ti'].xcom_push(key='df_dept', value=df_dept.to_json(orient='records', date_format='iso'))
    context['ti'].xcom_push(key='df_emp', value=df_emp.to_json(orient='records', date_format='iso'))
    context['ti'].xcom_push(key='df_date', value=df_date.to_json(orient='records', date_format='iso'))
    context['ti'].xcom_push(key='stg_ts_df', value=stg_ts_df.to_json(orient='records', date_format='iso'))
    context['ti'].xcom_push(key='skip_load', value=False)
    
    logger.info(f"Dimensions transformed - dept: {len(df_dept)}, emp: {len(df_emp)}, date: {len(df_date)}")


def load_dim_department_task(**context):
    """Load dimension department to Gold layer."""
    import pandas as pd
    
    skip = context['ti'].xcom_pull(key='skip_load', task_ids='load_and_transform_dimensions')
    if skip:
        logger.info("Skipping dim_department load - no staging data")
        return
    
    logger.info("Loading dim_department...")
    df_dept_json = context['ti'].xcom_pull(key='df_dept', task_ids='load_and_transform_dimensions')
    df_dept = pd.read_json(df_dept_json, orient='records')
    
    session = get_session()
    try:
        upsert_dataframe(df_dept, DimDepartment, session, key_cols=["department_key"])
        logger.info(f"Loaded {len(df_dept)} department records")
    finally:
        session.close()


def load_dim_employee_task(**context):
    """Load dimension employee to Gold layer."""
    import pandas as pd
    
    skip = context['ti'].xcom_pull(key='skip_load', task_ids='load_and_transform_dimensions')
    if skip:
        logger.info("Skipping dim_employee load - no staging data")
        return
    
    logger.info("Loading dim_employee...")
    df_emp_json = context['ti'].xcom_pull(key='df_emp', task_ids='load_and_transform_dimensions')
    df_emp = pd.read_json(df_emp_json, orient='records')
    
    session = get_session()
    try:
        upsert_dataframe(df_emp, DimEmployee, session, key_cols=["employee_key"])
        logger.info(f"Loaded {len(df_emp)} employee records")
    finally:
        session.close()


def load_dim_date_task(**context):
    """Load dimension date to Gold layer."""
    import pandas as pd
    
    skip = context['ti'].xcom_pull(key='skip_load', task_ids='load_and_transform_dimensions')
    if skip:
        logger.info("Skipping dim_date load - no staging data")
        return
    
    logger.info("Loading dim_date...")
    df_date_json = context['ti'].xcom_pull(key='df_date', task_ids='load_and_transform_dimensions')
    df_date = pd.read_json(df_date_json, orient='records')
    
    session = get_session()
    try:
        insert_new_dates(df_date, DimDate, session)
        logger.info(f"Loaded new date records")
    finally:
        session.close()


def transform_and_load_fact_task(**context):
    """Transform and load fact_timesheet to Gold layer."""
    import pandas as pd
    
    skip = context['ti'].xcom_pull(key='skip_load', task_ids='load_and_transform_dimensions')
    if skip:
        logger.info("Skipping fact_timesheet load - no staging data")
        return
    
    logger.info("Transforming and loading fact_timesheet...")
    
    # Pull data from XCom
    stg_ts_json = context['ti'].xcom_pull(key='stg_ts_df', task_ids='load_and_transform_dimensions')
    df_emp_json = context['ti'].xcom_pull(key='df_emp', task_ids='load_and_transform_dimensions')
    
    stg_ts_df = pd.read_json(stg_ts_json, orient='records')
    df_emp = pd.read_json(df_emp_json, orient='records')
    
    # Transform fact
    df_fact = transform_fact_timesheet(stg_ts_df, df_emp)
    
    # Load to Gold
    session = get_session()
    try:
        upsert_dataframe(df_fact, FactTimesheet, session, key_cols=["id"])
        logger.info(f"Loaded {len(df_fact)} fact_timesheet records")
    finally:
        session.close()


def refresh_fact_task():
    """Refresh fact_timesheet table (truncate and reload)."""
    logger.info("Refreshing fact_timesheet...")
    result = refresh_fact_timesheet()
    logger.info(f"Refresh complete: {result}")


# DAG definition
dag = DAG(
    'gold_layer_etl',
    default_args=default_args,
    description='Transform Silver staging data into Gold dimensional model',
    schedule='@daily',
    catchup=False,
    tags=['gold', 'etl', 'dimensional_model'],
)

# Task 1: Create Gold tables
create_tables = PythonOperator(
    task_id='create_gold_tables',
    python_callable=create_tables_task,
    dag=dag,
)

# Task 2: Load and transform dimensions
load_transform_dims = PythonOperator(
    task_id='load_and_transform_dimensions',
    python_callable=load_and_transform_dimensions_task,
    dag=dag,
)

# Task 3: Load dim_department
load_dept = PythonOperator(
    task_id='load_dim_department',
    python_callable=load_dim_department_task,
    dag=dag,
)

# Task 4: Load dim_employee
load_emp = PythonOperator(
    task_id='load_dim_employee',
    python_callable=load_dim_employee_task,
    dag=dag,
)

# Task 5: Load dim_date
load_date = PythonOperator(
    task_id='load_dim_date',
    python_callable=load_dim_date_task,
    dag=dag,
)

# Task 6: Transform and load fact_timesheet
load_fact = PythonOperator(
    task_id='load_fact_timesheet',
    python_callable=transform_and_load_fact_task,
    dag=dag,
)

# Task dependencies
create_tables >> load_transform_dims
load_transform_dims >> [load_dept, load_emp, load_date]
[load_dept, load_emp, load_date] >> load_fact


# Separate DAG for fact refresh (manual trigger)
refresh_dag = DAG(
    'gold_fact_refresh',
    default_args=default_args,
    description='Refresh fact_timesheet table (truncate and reload)',
    schedule=None,
    catchup=False,
    tags=['gold', 'refresh', 'maintenance'],
)

refresh_fact = PythonOperator(
    task_id='refresh_fact_timesheet',
    python_callable=refresh_fact_task,
    dag=refresh_dag,
)