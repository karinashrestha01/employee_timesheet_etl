"""
Gold Layer ETL - Load from Silver (staging) to Gold (final dimensional model).
Creates dimension and fact tables from cleaned staging data.
"""

import logging
from typing import Dict, Any

import pandas as pd
import numpy as np

from db.db_utils import get_engine, get_session, upsert_dataframe
from db.models import Base, DimEmployee, DimDepartment, DimDate, DimPayCode, FactTimesheet
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
    logger.info("Inserted new date records (skipped existing)")


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
    
    Note: department_key is NOT generated here - the database auto-increments it.
    We only prepare the natural key (department_id) and attributes.
    
    Args:
        emp_df: Staging employee DataFrame
        
    Returns:
        Transformed department dimension DataFrame (without department_key)
    """
    df_dept = emp_df[["department_id", "department_name"]].drop_duplicates().reset_index(drop=True)
    
    # Validate: ensure no null department_ids
    null_dept_count = df_dept["department_id"].isna().sum()
    if null_dept_count > 0:
        logger.warning(f"Found {null_dept_count} null department_ids - these will be excluded")
        df_dept = df_dept[df_dept["department_id"].notna()]
    
    logger.debug(f"Prepared {len(df_dept)} unique departments for upsert")
    return clean_nulls(df_dept)


def transform_dim_employee(emp_df: pd.DataFrame, dept_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create dimension employee from staging data.
    
    Note: employee_key is NOT generated here - the database auto-increments it.
    dept_df must contain actual DB-generated department_keys (from reload after insert).
    
    Args:
        emp_df: Staging employee DataFrame
        dept_df: Department dimension DataFrame WITH DB-generated department_key values
        
    Returns:
        Transformed employee dimension DataFrame (without employee_key)
    """
    # Validate dept_df has required columns
    required_cols = {"department_id", "department_key"}
    if not required_cols.issubset(dept_df.columns):
        raise ValueError(f"dept_df must contain columns: {required_cols}")
    
    # Merge with department to get department_key (FK)
    df = emp_df.merge(
        dept_df[["department_id", "department_key"]], 
        on="department_id", 
        how="left"
    )
    
    # Log employees with missing department
    orphan_count = df["department_key"].isna().sum()
    if orphan_count > 0:
        logger.warning(f"Found {orphan_count} employees without matching department")
    
    # Select columns for dimension (excluding employee_key - DB auto-generates)
    columns = [
        "employee_id", "first_name", "last_name",
        "job_title", "department_key", "hire_date", "termination_date",
        "is_active"
    ]
    
    df = df[[c for c in columns if c in df.columns]]
    
    # Convert department_key to int where not null
    if "department_key" in df.columns:
        df["department_key"] = df["department_key"].apply(
            lambda x: int(x) if pd.notna(x) else None
        )
    
    logger.debug(f"Prepared {len(df)} employees for upsert")
    return clean_nulls(df)


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


def transform_dim_pay_code(ts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create dimension pay_code from timesheet pay codes.
    
    Args:
        ts_df: Staging timesheet DataFrame
        
    Returns:
        Transformed pay code dimension DataFrame (without pay_code_key - let DB auto-generate)
    """
    # Get unique pay codes (excluding empty/null values)
    pay_codes = ts_df[ts_df["pay_code"].notna() & (ts_df["pay_code"] != "")]["pay_code"].unique()
    
    df = pd.DataFrame({"pay_code": pay_codes})
    
    # Add descriptive information based on common pay code patterns
    # This is a placeholder - you can enhance this with actual business logic
    df["pay_code_description"] = df["pay_code"].apply(lambda x: f"{x} Pay")
    
    # Determine if it's overtime or PTO based on common patterns
    # Adjust these patterns based on your actual pay code conventions
    df["is_overtime"] = df["pay_code"].str.contains("OT|OVERTIME", case=False, na=False).astype(int)
    df["is_pto"] = df["pay_code"].str.contains("PTO|VACATION|SICK", case=False, na=False).astype(int)
    
    return clean_nulls(df)


# FACT TRANSFORMATION

def transform_fact_timesheet(ts_df: pd.DataFrame, emp_df: pd.DataFrame, pay_code_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Create fact timesheet from staging data.
    
    Args:
        ts_df: Staging timesheet DataFrame
        emp_df: Employee dimension DataFrame (for key lookup)
        pay_code_df: Pay code dimension DataFrame (for key lookup, optional)
        
    Returns:
        Transformed fact timesheet DataFrame with derived metrics
    """
    original_count = len(ts_df)
    
    # LEFT JOIN to preserve timesheets even if employee not found
    # This prevents silent data loss from inner join
    df = ts_df.merge(
        emp_df[["employee_id", "employee_key", "department_key"]],
        on="employee_id",
        how="left"  # Changed from 'inner' to prevent data loss
    )
    
    # Log and handle orphan timesheets (no matching employee)
    orphan_count = df["employee_key"].isna().sum()
    if orphan_count > 0:
        logger.warning(f"Found {orphan_count} timesheets without matching employee (out of {original_count})")
        # Filter out orphans - employee_key is required FK
        df = df[df["employee_key"].notna()]
        logger.info(f"Keeping {len(df)} timesheets with valid employee_key")
    
    # Parse datetime columns - MUST be done before any .dt accessor usage
    df["work_date"] = pd.to_datetime(df["work_date"], errors="coerce")
    df["scheduled_start_datetime"] = pd.to_datetime(df["scheduled_start_datetime"], errors="coerce")
    df["scheduled_end_datetime"] = pd.to_datetime(df["scheduled_end_datetime"], errors="coerce")
    df["punch_in"] = pd.to_datetime(df["punch_in"], errors="coerce")
    df["punch_out"] = pd.to_datetime(df["punch_out"], errors="coerce")
    
    # Log null scheduled datetimes (source data issue tracking)
    null_sched_start = df["scheduled_start_datetime"].isna().sum()
    null_sched_end = df["scheduled_end_datetime"].isna().sum()
    if null_sched_start > 0 or null_sched_end > 0:
        logger.info(f"Scheduled datetime nulls: start={null_sched_start}, end={null_sched_end} "
                   f"(source data may not provide schedule information)")
    
    # Calculate derived metrics
    # hours_scheduled: scheduled_end - scheduled_start in hours
    df["hours_scheduled"] = (
        (df["scheduled_end_datetime"] - df["scheduled_start_datetime"])
        .dt.total_seconds() / 3600
    ).where(df["scheduled_start_datetime"].notna() & df["scheduled_end_datetime"].notna(), 0)
    
    # hours_variance: hours_worked - hours_scheduled
    # df["hours_variance"] = df["hours_worked"] - df["hours_scheduled"]
    
    # is_late_arrival: 1 if punch_in > scheduled_start
    df["is_late_arrival"] = (
        (df["punch_in"] > df["scheduled_start_datetime"]).astype(int)
    ).where(df["punch_in"].notna() & df["scheduled_start_datetime"].notna(), 0)
    
    # is_early_departure: 1 if punch_out < scheduled_end
    df["is_early_departure"] = (
        (df["punch_out"] < df["scheduled_end_datetime"]).astype(int)
    ).where(df["punch_out"].notna() & df["scheduled_end_datetime"].notna(), 0)
    
    # late_arrival_minutes
    df["late_arrival_minutes"] = (
        (df["punch_in"] - df["scheduled_start_datetime"])
        .dt.total_seconds() / 60
    ).where(
        (df["punch_in"].notna() & df["scheduled_start_datetime"].notna() & 
         (df["punch_in"] > df["scheduled_start_datetime"])), 
        0.0
    )
    
    # early_departure_minutes
    df["early_departure_minutes"] = (
        (df["scheduled_end_datetime"] - df["punch_out"])
        .dt.total_seconds() / 60
    ).where(
        (df["punch_out"].notna() & df["scheduled_end_datetime"].notna() & 
         (df["punch_out"] < df["scheduled_end_datetime"])), 
        0.0
    )
    # breakpoint()
    # Merge with pay_code dimension if provided
    if pay_code_df is not None and "pay_code" in df.columns:
        df = df.merge(
            pay_code_df[["pay_code", "pay_code_key"]],
            on="pay_code",
            how="left"
        )
    else:
        df["pay_code_key"] = None
    
    # Apply comment categorization
    if "punch_in_comment" in df.columns:
        df["punch_in_comment"] = df["punch_in_comment"].apply(clean_comment)
    if "punch_out_comment" in df.columns:
        df["punch_out_comment"] = df["punch_out_comment"].apply(clean_comment)
    
    # Select fact columns (matching model schema)
    columns = [
        "employee_key", "department_key", "pay_code_key", "work_date", 
        "punch_in", "punch_out", "scheduled_start_datetime", "scheduled_end_datetime",
        "hours_worked", "hours_scheduled",
        "is_late_arrival", "is_early_departure", 
        "late_arrival_minutes", "early_departure_minutes",
        "punch_in_comment", "punch_out_comment"
    ]
    # breakpoint()
    df = df[[c for c in columns if c in df.columns]]
    
    # Convert foreign keys to int, handling NaN
    for key_col in ["department_key", "pay_code_key"]:
        if key_col in df.columns:
            df[key_col] = df[key_col].apply(
                lambda x: int(x) if pd.notna(x) else None
            )
    
    # Convert employee_key to int (required, should not have NaN at this point)
    df["employee_key"] = df["employee_key"].astype(int)
    # breakpoint()
    
    # Deduplicate based on unique constraint columns to prevent CardinalityViolation
    # Keep the last occurrence (most recent data)
    initial_count = len(df)
    df = df.drop_duplicates(subset=["employee_key", "work_date"], keep="last")
    if len(df) < initial_count:
        logger.info(f"Removed {initial_count - len(df)} duplicate timesheet records")

    return clean_nulls(df)


# MAIN ETL FUNCTION

def run_gold_load() -> Dict[str, Any]:
    """
    Run the complete Gold layer ETL.
    Transforms staging data into dimensional model.
    
    Process Flow:
    1. Load staging data
    2. Upsert dim_department (using department_id as natural key)
    3. Reload dim_department to get DB-generated department_keys
    4. Upsert dim_employee (using employee_id as natural key) with correct department_keys
    5. Reload dim_employee to get DB-generated employee_keys
    6. Upsert dim_date and dim_pay_code
    7. Reload dim_pay_code to get DB-generated pay_code_keys
    8. Transform and upsert fact_timesheet with correct foreign keys
    
    Returns:
        dict: Load statistics for each table
    """
    logger.info("=" * 60)
    logger.info("GOLD LAYER: Loading dimensional model")
    logger.info("=" * 60)
    
    engine = get_engine()
    create_gold_tables(engine)
    
    session = get_session()
    stats = {
        "status": "success",
        "dim_department": 0,
        "dim_employee": 0,
        "dim_date": 0,
        "dim_pay_code": 0,
        "fact_timesheet": 0
    }
    
    try:

        # STEP 1: Load staging data

        logger.info("-" * 40)
        logger.info("STEP 1: Loading data from staging...")
        stg_emp_df = load_staging_employees(engine)
        stg_ts_df = load_staging_timesheets(engine)

        if stg_emp_df.empty:
            logger.warning("No employee data in staging - skipping Gold load")
            return {"status": "skipped", "reason": "no staging data"}
        
        logger.info(f"  Loaded {len(stg_emp_df)} employees, {len(stg_ts_df)} timesheets from staging")
        

        # STEP 2: Transform and upsert dim_department
        logger.info("-" * 40)
        logger.info("STEP 2: Processing dim_department...")
        
        df_dept = transform_dim_department(stg_emp_df)
        logger.info(f"  Transformed {len(df_dept)} unique departments")
        
        # Upsert using department_id (natural key) - NOT department_key
        logger.info("  Upserting dim_department (key: department_id)")
        upsert_dataframe(df_dept, DimDepartment, session, key_cols=["department_id"])
        stats["dim_department"] = len(df_dept)
        

        # STEP 3: Reload dim_department with DB-generated keys

        logger.info("-" * 40)
        logger.info("STEP 3: Reloading dim_department with DB-generated keys...")
        
        df_dept_with_keys = pd.read_sql(
            "SELECT department_key, department_id, department_name FROM dim_department", 
            engine
        )
        logger.info(f"  Reloaded {len(df_dept_with_keys)} departments with keys")
        

        # STEP 4: Transform and upsert dim_employee

        logger.info("-" * 40)
        logger.info("STEP 4: Processing dim_employee...")
        
        # Pass the reloaded department data with actual DB keys
        df_emp = transform_dim_employee(stg_emp_df, df_dept_with_keys)
        logger.info(f"  Transformed {len(df_emp)} employees")
        
        # Upsert using employee_id (natural key) - NOT employee_key
        logger.info("  Upserting dim_employee (key: employee_id)...")
        upsert_dataframe(df_emp, DimEmployee, session, key_cols=["employee_id"])
        stats["dim_employee"] = len(df_emp)
        

        # STEP 5: Reload dim_employee with DB-generated keys

        logger.info("-" * 40)
        logger.info("STEP 5: Reloading dim_employee with DB-generated keys...")
        
        df_emp_with_keys = pd.read_sql(
            "SELECT employee_key, employee_id, department_key FROM dim_employee", 
            engine
        )
        logger.info(f"Reloaded {len(df_emp_with_keys)} employees with keys")
        

        # STEP 6: Transform and upsert dim_date and dim_pay_code

        logger.info("-" * 40)
        logger.info("STEP 6: Processing dim_date and dim_pay_code...")
        
        # dim_date
        df_date = transform_dim_date(stg_ts_df)
        logger.info(f"  Transformed {len(df_date)} unique dates")
        logger.info("  Inserting new dim_date records...")
        insert_new_dates(df_date, DimDate, session)
        stats["dim_date"] = len(df_date)
        
        # dim_pay_code
        df_pay_code = transform_dim_pay_code(stg_ts_df)
        logger.info(f"  Transformed {len(df_pay_code)} unique pay codes")
        logger.info("  Upserting dim_pay_code (key: pay_code)...")
        upsert_dataframe(df_pay_code, DimPayCode, session, key_cols=["pay_code"])
        stats["dim_pay_code"] = len(df_pay_code)
        

        # STEP 7: Reload dim_pay_code with DB-generated keys

        logger.info("-" * 40)
        logger.info("STEP 7: Reloading dim_pay_code with DB-generated keys...")
        
        df_pay_code_with_keys = pd.read_sql(
            "SELECT pay_code_key, pay_code FROM dim_pay_code", 
            engine
        )
        logger.info(f"  Reloaded {len(df_pay_code_with_keys)} pay codes with keys")
        

        # STEP 8: Transform and upsert fact_timesheet

        logger.info("-" * 40)
        logger.info("STEP 8: Processing fact_timesheet...")
        
        # Transform fact with DB-reloaded employee and pay_code keys
        df_fact = transform_fact_timesheet(stg_ts_df, df_emp_with_keys, df_pay_code_with_keys)
        logger.info(f"  Transformed {len(df_fact)} timesheet records")
        
        logger.info("  Upserting fact_timesheet (key: employee_key, work_date)...")
        upsert_dataframe(df_fact, FactTimesheet, session, key_cols=["employee_key", "work_date"])
        stats["fact_timesheet"] = len(df_fact)
        

        # COMPLETE

        logger.info("=" * 60)
        logger.info("GOLD LAYER COMPLETE")
        logger.info(f"  Departments: {stats['dim_department']}")
        logger.info(f"  Employees:   {stats['dim_employee']}")
        logger.info(f"  Dates:       {stats['dim_date']}")
        logger.info(f"  Pay Codes:   {stats['dim_pay_code']}")
        logger.info(f"  Timesheets:  {stats['fact_timesheet']}")
        logger.info("=" * 60)
        
        return stats
        
    except Exception as e:
        logger.error(f"Gold layer ETL failed: {e}")
        stats["status"] = "failed"
        stats["error"] = str(e)
        raise
        
    finally:
        session.close()


if __name__ == "__main__":
    from ETL.common.logging import configure_logging
    configure_logging()
    run_gold_load()