# ETL/silver/transformer.py
"""
Silver Layer ETL - Transform Bronze data to cleaned staging tables.
Implements incremental loading based on watermark timestamps.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
from uuid import uuid4

import pandas as pd
from sqlalchemy import text

# Ensure project root is in sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from db.db_utils import get_engine, get_session
from db.models_bronze import RawEmployee, RawTimesheet
from db.models_silver import SilverBase, StagingEmployee, StagingTimesheet, ETLWatermark
from ETL.silver.validator import run_silver_validation

# Import cleaning utilities from silver_utils
from ETL.silver.utils import (
    clean_string_column,
    clean_numeric_column,
    clean_date_column,
    clean_date_column_with_sentinel,
    clean_comment,
    SENTINEL_END_DATE,
    NULL_PLACEHOLDERS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# =============================================================================
# SCHEMA AND TABLE MANAGEMENT
# =============================================================================

def create_staging_schema(engine):
    """Create the 'staging' schema if it doesn't exist."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
        conn.commit()
    logger.info("Schema 'staging' created or already exists")


def create_silver_tables(engine):
    """Create Silver layer tables."""
    create_staging_schema(engine)
    SilverBase.metadata.create_all(engine)
    logger.info("Silver tables created successfully")


# =============================================================================
# WATERMARK MANAGEMENT (Incremental Loading)
# =============================================================================

def get_watermark(session, table_name: str) -> datetime:
    """Get the last processed watermark for a table."""
    result = session.query(ETLWatermark).filter_by(table_name=table_name).first()
    if result:
        return result.last_processed_at
    return datetime.min


def update_watermark(session, table_name: str, timestamp: datetime):
    """Update the watermark for a table."""
    existing = session.query(ETLWatermark).filter_by(table_name=table_name).first()
    if existing:
        existing.last_processed_at = timestamp
        existing.updated_at = datetime.utcnow()
    else:
        new_watermark = ETLWatermark(
            table_name=table_name,
            last_processed_at=timestamp
        )
        session.add(new_watermark)
    session.commit()


# =============================================================================
# DATA CLEANING FUNCTIONS
# =============================================================================

def clean_employee_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform raw employee data.
    Handles null values for all fields.
    """
    # Rename columns to staging format
    df = df.rename(columns={"client_employee_id": "employee_id"})
    
    # String columns
    df["employee_id"] = clean_string_column(df["employee_id"], default_value="UNKNOWN")
    df["first_name"] = clean_string_column(df["first_name"], default_value="")
    df["last_name"] = clean_string_column(df["last_name"], default_value="")
    df["job_title"] = clean_string_column(df["job_title"], default_value="Unknown")
    
    if "department_id" in df.columns:
        df["department_id"] = clean_string_column(df["department_id"], default_value=None)
    if "department_name" in df.columns:
        df["department_name"] = clean_string_column(df["department_name"], default_value="Unknown")
    if "source_file" in df.columns:
        df["source_file"] = clean_string_column(df["source_file"], default_value=None)
    
    # Date columns
    if "hire_date" in df.columns:
        df["hire_date"] = clean_date_column(df["hire_date"])
    else:
        df["hire_date"] = pd.NaT
    
    # Termination date with sentinel
    if "term_date" in df.columns:
        df["termination_date"] = clean_date_column_with_sentinel(df["term_date"])
    elif "termination_date" in df.columns:
        df["termination_date"] = clean_date_column_with_sentinel(df["termination_date"])
    else:
        df["termination_date"] = SENTINEL_END_DATE
    
    # Calculate is_active
    df["is_active"] = (df["termination_date"] == SENTINEL_END_DATE).astype(int)
    
    # Select final columns
    columns = [
        "employee_id", "first_name", "last_name", "job_title",
        "department_id", "department_name", "hire_date", "termination_date",
        "is_active", "source_file", "loaded_at"
    ]
    df = df[[c for c in columns if c in df.columns]]
    df = df.rename(columns={"loaded_at": "bronze_loaded_at"})
    
    logger.info(f"Employee data cleaned: {len(df)} records")
    return df


def clean_timesheet_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform raw timesheet data.
    Handles null values for all fields.
    """
    # breakpoint()
    # Rename columns
    df = df.rename(columns={
        "client_employee_id": "employee_id",
        "punch_apply_date": "work_date",
        "punch_in_datetime": "punch_in",
        "punch_out_datetime": "punch_out"
    })

    # String columns
    df["employee_id"] = clean_string_column(df["employee_id"], default_value="UNKNOWN")
    df["pay_code"] = clean_string_column(df["pay_code"], default_value="")
    if "source_file" in df.columns:
        df["source_file"] = clean_string_column(df["source_file"], default_value=None)
    
    # DateTime columns
    df["work_date"] = clean_date_column(df["work_date"]) if "work_date" in df.columns else pd.NaT
    df["punch_in"] = clean_date_column(df["punch_in"]) if "punch_in" in df.columns else pd.NaT
    df["punch_out"] = clean_date_column(df["punch_out"]) if "punch_out" in df.columns else pd.NaT

    # Numeric columns
    df["hours_worked"] = clean_numeric_column(df["hours_worked"], default_value=0.0) if "hours_worked" in df.columns else 0.0

    # Comment columns - categorized
    df["punch_in_comment"] = df["punch_in_comment"].apply(clean_comment) if "punch_in_comment" in df.columns else ""
    # breakpoint()
    df["punch_out_comment"] = df["punch_out_comment"].apply(clean_comment) if "punch_out_comment" in df.columns else ""

    # category for comment columns
    df["punch_in_category"] = df["punch_in_comment"].apply(clean_comment) if "punch_in_category" in df.columns else ""
    df["punch_out_category"] = df["punch_out_comment"].apply(clean_comment) if "punch_out_category" in df.columns else ""
    # print(f"---------------CATEGORY COMMENT : {df['punch_in_category']}")
    # breakpoint()

    # Select final columns
    columns = [
        "employee_id", "work_date", "punch_in", "punch_out",
        "hours_worked", "pay_code", "punch_in_comment", "punch_out_comment",
        "source_file", "loaded_at"
    ]
    df = df[[c for c in columns if c in df.columns]]
    df = df.rename(columns={"loaded_at": "bronze_loaded_at"})
    
    logger.info(f"Timesheet data cleaned: {len(df)} records")
    return df


# =============================================================================
# INCREMENTAL LOADING FUNCTIONS
# =============================================================================

def load_incremental_employees(session, engine, batch_id: str) -> pd.DataFrame:
    """Load and transform new employee records from Bronze layer."""
    watermark = get_watermark(session, "raw_employee")
    logger.info(f"Employee watermark: {watermark}")
    
    query = f"""
        SELECT * FROM raw.raw_employee
        WHERE loaded_at > '{watermark}'
        ORDER BY loaded_at
    """
    df = pd.read_sql(query, engine)
    
    if df.empty:
        logger.info("No new employee records to process")
        return pd.DataFrame()
    
    logger.info(f"Found {len(df)} new employee records")
    
    df = clean_employee_data(df)
    df["etl_batch_id"] = batch_id
    df["processed_at"] = datetime.utcnow()
    
    max_loaded = df["bronze_loaded_at"].max()
    if pd.notna(max_loaded):
        update_watermark(session, "raw_employee", max_loaded)
    
    return df


def load_incremental_timesheets(session, engine, batch_id: str) -> pd.DataFrame:
    """Load and transform new timesheet records from Bronze layer."""
    watermark = get_watermark(session, "raw_timesheet")
    logger.info(f"Timesheet watermark: {watermark}")
    # query = f"""
    #     SELECT * FROM raw.raw_timesheet
    #     WHERE loaded_at > '{watermark}'
    #     ORDER BY loaded_at
    # """
    watermark = watermark.strftime("%Y-%m-%d %H:%M:%S")
    query = f"""
        SELECT * FROM raw.raw_timesheet
        WHERE loaded_at > '{watermark}'
        ORDER BY loaded_at
    """
    df = pd.read_sql(query, engine)
    if df.empty:
        logger.info("No new timesheet records to process")
        return pd.DataFrame()
    
    logger.info(f"Found {len(df)} new timesheet records")
    df = clean_timesheet_data(df)
    df["etl_batch_id"] = batch_id
    df["processed_at"] = datetime.utcnow()
    
    max_loaded = df["bronze_loaded_at"].max()
    if pd.notna(max_loaded):
        update_watermark(session, "raw_timesheet", max_loaded)
    
    return df


def insert_staging_data(df: pd.DataFrame, table_class, session, batch_size: int = 1000):
    """Insert data into staging tables."""
    if df.empty:
        return 0
    
    # Clean NaN/NaT values
    df = df.replace({pd.NaT: None, pd.NA: None})
    df = df.where(pd.notna(df), None)
    
    records = df.to_dict(orient="records")
    total = len(records)
    
    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        session.bulk_insert_mappings(table_class, batch)
        session.commit()
        logger.info(f"  Inserted batch {i // batch_size + 1} ({len(batch)} records)")
    
    return total


# =============================================================================
# MAIN ETL FUNCTION
# =============================================================================

def run_silver_transform(validate: bool = True):
    """
    Run the complete Silver layer ETL with incremental loading.
    
    Args:
        validate: If True, run validation checks on staging data
    
    Returns:
        dict: Load statistics and validation results
    """
    logger.info("=" * 60)
    logger.info("SILVER LAYER: Transforming to staging")
    logger.info("=" * 60)
    
    engine = get_engine()
    create_silver_tables(engine)
    
    session = get_session()
    batch_id = str(uuid4())[:8]
    logger.info(f"ETL Batch ID: {batch_id}")
    try:
        # Process employees
        logger.info("-" * 40)
        logger.info("Processing employees...")
        emp_df = load_incremental_employees(session, engine, batch_id)
        emp_count = insert_staging_data(emp_df, StagingEmployee, session)
        
        # Process timesheets
        logger.info("-" * 40)
        logger.info("Processing timesheets...")
        ts_df = load_incremental_timesheets(session, engine, batch_id)
        
        # Filter out orphan timesheets (employee_ids not in employee table)
        if not ts_df.empty:
            # Get all valid employee_ids from staging
            all_emp_ids = pd.read_sql(
                "SELECT DISTINCT employee_id FROM staging.stg_employee", 
                engine
            )["employee_id"].tolist()
            
            original_count = len(ts_df)
            ts_df = ts_df[ts_df["employee_id"].isin(all_emp_ids)]
            filtered_count = original_count - len(ts_df)
            
            if filtered_count > 0:
                logger.warning(f"Filtered out {filtered_count} orphan timesheet records "
                             f"(employee_ids not found in stg_employee)")
            logger.info(f"Keeping {len(ts_df)} valid timesheet records")
        
        ts_count = insert_staging_data(ts_df, StagingTimesheet, session)
        
        # Validation
        validation_reports = []
        if validate and (not emp_df.empty or not ts_df.empty):
            all_emp_df = pd.read_sql("SELECT * FROM staging.stg_employee", engine)
            all_ts_df = pd.read_sql("SELECT * FROM staging.stg_timesheet", engine)
            validation_reports = run_silver_validation(all_emp_df, all_ts_df)
        
        logger.info("=" * 60)
        logger.info(f"SILVER LAYER COMPLETE: {emp_count} employees, {ts_count} timesheets processed")
        logger.info("=" * 60)
        
        return {
            "batch_id": batch_id,
            "employees": emp_count,
            "timesheets": ts_count,
            "validation": validation_reports
        }
    # except:
    #     breakpoint()
    finally:
        session.close()


if __name__ == "__main__":
    run_silver_transform()
