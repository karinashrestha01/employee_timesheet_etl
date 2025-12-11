# ETL/gold/refresh.py
"""
Refresh Gold layer fact_timesheet with updated transformations.
Truncates and reloads the fact_timesheet table only.
"""

import logging
from typing import Dict, Any

from sqlalchemy import text

from db.db_utils import get_engine, get_session, upsert_dataframe
from db.models import FactTimesheet
from ETL.gold.loader import (
    load_staging_employees, 
    load_staging_timesheets,
    transform_dim_employee,
    transform_dim_department,
    transform_fact_timesheet
)

logger = logging.getLogger(__name__)


def refresh_fact_timesheet() -> Dict[str, Any]:
    """
    Truncate and reload fact_timesheet with proper transformations.
    
    This function is useful when transformation logic has changed
    and you need to reprocess all fact records without touching
    dimension tables.
    
    Returns:
        dict: Refresh status and record count
    """
    logger.info("=" * 60)
    logger.info("REFRESHING GOLD FACT_TIMESHEET")
    logger.info("=" * 60)
    
    engine = get_engine()
    session = get_session()
    
    try:
        # Step 1: Truncate fact_timesheet
        logger.info("Truncating fact_timesheet table...")
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE public.fact_timesheet RESTART IDENTITY"))
            conn.commit()
        logger.info("fact_timesheet truncated successfully")
        
        # Step 2: Load staging data
        logger.info("Loading staging data...")
        stg_emp_df = load_staging_employees(engine)
        stg_ts_df = load_staging_timesheets(engine)
        
        if stg_emp_df.empty or stg_ts_df.empty:
            logger.warning("No staging data available")
            return {"status": "skipped", "reason": "no staging data"}
        
        # Step 3: Transform dimensions (needed to get keys)
        logger.info("Transforming dimension tables for key mapping...")
        df_dept = transform_dim_department(stg_emp_df)
        df_emp = transform_dim_employee(stg_emp_df, df_dept)
        
        # Step 4: Transform fact_timesheet
        logger.info("Transforming fact_timesheet...")
        df_fact = transform_fact_timesheet(stg_ts_df, df_emp)
        logger.info(f"Transformed {len(df_fact)} fact records")
        
        # Step 5: Insert to Gold
        logger.info("Inserting to fact_timesheet...")
        upsert_dataframe(df_fact, FactTimesheet, session, key_cols=["id"])
        
        logger.info("=" * 60)
        logger.info(f"REFRESH COMPLETE: {len(df_fact)} records loaded")
        logger.info("=" * 60)
        
        return {"status": "success", "records": len(df_fact)}
        
    except Exception as e:
        logger.error(f"Error refreshing fact_timesheet: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    from ETL.common.logging import configure_logging
    configure_logging()
    refresh_fact_timesheet()
