"""
Gold Layer ETL - Load from Silver (staging) to Gold (final dimensional model).
Creates dimension and fact tables from cleaned staging data.
"""

import logging
from datetime import date
from typing import Dict, Any

import pandas as pd
import numpy as np

from db.db_utils import get_engine, get_session, upsert_dataframe
from db.models import Base, DimEmployee, DimDepartment, DimDate, FactTimesheet
from ETL.silver.utils import clean_comment
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger(__name__)


# CONSTANTS

# Sentinel date for "no end date" - industry standard for SCD2
SENTINEL_END_DATE = pd.to_datetime("2222-12-01")


# TABLE MANAGEMENT


def create_gold_tables(engine) -> None:
    """Create Gold layer tables (public schema)."""
    Base.metadata.create_all(engine)
    logger.info("Gold tables created successfully")


# DATA LOADING FROM STAGING

def clean_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Replace NaN/NaT with None for database insertion."""
    df = df.replace({pd.NaT: None, pd.NA: None, np.nan: None})
    return df.where(pd.notna(df), None)


def insert_new_dates(df: pd.DataFrame, table_class, session, batch_size: int = 500):
    """
    Insert new date records, ignoring conflicts on work_date.
    Date dimension doesn't need updates once created.
    """
    df = clean_nulls(df)
    records = df.to_dict(orient="records")
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        stmt = insert(table_class).values(batch)
        # ON CONFLICT (work_date) DO NOTHING - just skip existing dates
        stmt = stmt.on_conflict_do_nothing(index_elements=["work_date"])
        session.execute(stmt)
        session.commit()
        
    from db.db_utils import logger
    logger.info(f"Inserted new date records (skipped existing)")


def load_staging_employees(engine) -> pd.DataFrame:
    """Load employee data from staging layer."""
    query = "SELECT * FROM staging.stg_employee"
    df = pd.read_sql(query, engine)
    logger.info(f"Loaded {len(df)} employees from staging")
    return df


def load_staging_timesheets(engine) -> pd.DataFrame:
    """Load timesheet data from staging layer."""
    query = "SELECT * FROM staging.stg_timesheet"
    df = pd.read_sql(query, engine)
    logger.info(f"Loaded {len(df)} timesheets from staging")
    return df


# DIMENSION TRANSFORMATIONS

def transform_dim_department(emp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create dimension department from staging employee data.
    
    Args:
        emp_df: Staging employee DataFrame
        
    Returns:
        Transformed department dimension DataFrame
    """
    today = pd.to_datetime(date.today())
    
    df_dept = emp_df[["department_id", "department_name"]].drop_duplicates().reset_index(drop=True)
    df_dept = df_dept.reset_index()
    df_dept.rename(columns={"index": "department_key"}, inplace=True)
    df_dept["department_key"] += 1
    df_dept["is_active"] = 1
    df_dept["start_date"] = today
    df_dept["end_date"] = SENTINEL_END_DATE
    
    return clean_nulls(df_dept)


def transform_dim_employee(emp_df: pd.DataFrame, dept_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create dimension employee from staging data.
    
    Args:
        emp_df: Staging employee DataFrame
        dept_df: Department dimension DataFrame (for key lookup)
        
    Returns:
        Transformed employee dimension DataFrame
    """
    today = pd.to_datetime(date.today())
    
    # Merge with department to get department_key
    df = emp_df.merge(
        dept_df[["department_id", "department_key"]], 
        on="department_id", 
        how="left"
    )
    
    # Assign employee keys
    df["employee_key"] = range(1, len(df) + 1)
    df["start_date"] = today
    df["end_date"] = SENTINEL_END_DATE
    
    # Select columns for dimension
    columns = [
        "employee_key", "employee_id", "first_name", "last_name",
        "job_title", "department_key", "hire_date", "termination_date",
        "is_active", "start_date", "end_date"
    ]
    
    return clean_nulls(df[columns])


def transform_dim_date(ts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create dimension date from timesheet work dates.
    
    Args:
        ts_df: Staging timesheet DataFrame
        
    Returns:
        Transformed date dimension DataFrame (without date_id - let DB auto-generate)
    """
    df = pd.DataFrame({"work_date": ts_df["work_date"].dropna().unique()})
    df["work_date"] = pd.to_datetime(df["work_date"])
    df = df.sort_values("work_date").reset_index(drop=True)
    
    # Don't generate date_id - let database auto-increment
    df["year"] = df["work_date"].dt.year
    df["month"] = df["work_date"].dt.month
    df["day"] = df["work_date"].dt.day
    df["week"] = df["work_date"].dt.isocalendar().week.astype(int)
    df["quarter"] = df["work_date"].dt.quarter
    
    return clean_nulls(df)


# FACT TRANSFORMATION

def transform_fact_timesheet(ts_df: pd.DataFrame, emp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create fact timesheet from staging data.
    
    Args:
        ts_df: Staging timesheet DataFrame
        emp_df: Employee dimension DataFrame (for key lookup)
        
    Returns:
        Transformed fact timesheet DataFrame
    """
    # Merge with employee to get employee_key and department_key
    df = ts_df.merge(
        emp_df[["employee_id", "employee_key", "department_key"]],
        on="employee_id",
        how="inner"
    )
    
    # Add scheduled times (default values)
    df["scheduled_start"] = "09:00:00"
    df["scheduled_end"] = "17:00:00"
    
    # Apply comment categorization
    if "punch_in_comment" in df.columns:
        df["punch_in_comment"] = df["punch_in_comment"].apply(clean_comment)
    if "punch_out_comment" in df.columns:
        df["punch_out_comment"] = df["punch_out_comment"].apply(clean_comment)
    
    # Select fact columns
    columns = [
        "employee_key", "department_key", "work_date", 
        "punch_in", "punch_out", "scheduled_start", "scheduled_end",
        "hours_worked", "pay_code", "punch_in_comment", "punch_out_comment"
    ]
    
    df = df[[c for c in columns if c in df.columns]]
    
    # Convert department_key to int, handling NaN
    if "department_key" in df.columns:
        df["department_key"] = df["department_key"].apply(
            lambda x: int(x) if pd.notna(x) else None
        )
    
    return clean_nulls(df)


# MAIN ETL FUNCTION

def run_gold_load() -> Dict[str, Any]:
    """
    Run the complete Gold layer ETL.
    Transforms staging data into dimensional model.
    
    Returns:
        dict: Load statistics for each table
    """
    logger.info("=" * 60)
    logger.info("GOLD LAYER: Loading dimensional model")
    logger.info("=" * 60)
    
    engine = get_engine()
    create_gold_tables(engine)
    
    session = get_session()
    
    try:
        # Load staging data
        logger.info("-" * 40)
        logger.info("Loading data from staging...")
        stg_emp_df = load_staging_employees(engine)
        stg_ts_df = load_staging_timesheets(engine)

        if stg_emp_df.empty:
            logger.warning("No employee data in staging - skipping Gold load")
            return {"status": "skipped", "reason": "no staging data"}
        
        # Transform to dimensional model
        logger.info("-" * 40)
        logger.info("Transforming to dimensional model...")
        
        df_dept = transform_dim_department(stg_emp_df)
        logger.info(f"  dim_department: {len(df_dept)} records")
        
        df_emp = transform_dim_employee(stg_emp_df, df_dept)
        logger.info(f"  dim_employee: {len(df_emp)} records")
        
        df_date = transform_dim_date(stg_ts_df)
        logger.info(f"  dim_date: {len(df_date)} records")
        
        df_fact = transform_fact_timesheet(stg_ts_df, df_emp)
        logger.info(f"  fact_timesheet: {len(df_fact)} records")
        
        # Load to Gold tables
        logger.info("-" * 40)
        logger.info("Upserting to Gold tables...")
        
        logger.info("  Upserting dim_department...")
        upsert_dataframe(df_dept, DimDepartment, session, key_cols=["department_key"])
        
        logger.info("  Upserting dim_employee...")
        upsert_dataframe(df_emp, DimEmployee, session, key_cols=["employee_key"])
        
        logger.info("  Inserting new dim_date records...")
        insert_new_dates(df_date, DimDate, session)
        
        logger.info("  Upserting fact_timesheet...")
        upsert_dataframe(df_fact, FactTimesheet, session, key_cols=["id"])
        
        logger.info("=" * 60)
        logger.info("GOLD LAYER COMPLETE")
        logger.info("=" * 60)
        
        return {
            "status": "success",
            "dim_department": len(df_dept),
            "dim_employee": len(df_emp),
            "dim_date": len(df_date),
            "fact_timesheet": len(df_fact)
        }
        
    finally:
        session.close()


if __name__ == "__main__":
    from ETL.common.logging import configure_logging
    configure_logging()
    run_gold_load()
