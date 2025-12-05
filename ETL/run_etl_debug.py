# run_etl_debug.py
import sys
import os
from pathlib import Path
import logging
import traceback

# Ensure project root is in sys.path for imports
sys.path.append(str(Path(__file__).resolve().parent.parent))
from db.db_utils import create_all_tables, get_session, upsert_dataframe
from ETL.transform_clean import transform_data
from ETL.quality_checks import run_quality_checks, validate_post_load
from db.models import DimEmployee, DimDepartment, DimDate, FactTimesheet

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def run_etl_debug(download_dir="datasets", skip_on_qc_fail=False):
    """
    Run ETL pipeline with quality checks.
    
    Args:
        download_dir: Directory containing source CSV files
        skip_on_qc_fail: If True, abort ETL if quality checks fail
    """
    try:
        # Step 1: Create tables
        logger.info("=" * 60)
        logger.info("STEP 1: CREATING TABLES")
        logger.info("=" * 60)
        create_all_tables()
        
        # Step 2: Transform data
        logger.info("=" * 60)
        logger.info("STEP 2: TRANSFORMING DATA")
        logger.info("=" * 60)
        df_emp, df_dept, df_date, df_fact = transform_data(download_dir)
        
        # Step 3: Pre-load Quality Checks
        logger.info("=" * 60)
        logger.info("STEP 3: PRE-LOAD QUALITY CHECKS")
        logger.info("=" * 60)
        qc_report = run_quality_checks(df_emp, df_dept, df_date, df_fact)
        
        if not qc_report.passed:
            logger.warning(f"Quality checks failed: {qc_report.failed_count} issues found")
            if skip_on_qc_fail:
                raise ValueError(f"ETL aborted due to {qc_report.failed_count} QC failures")
            logger.warning("Proceeding with load despite QC failures...")
        else:
            logger.info("All quality checks passed!")
        
        # Step 4: Load data
        logger.info("=" * 60)
        logger.info("STEP 4: LOADING DATA")
        logger.info("=" * 60)
        session = get_session()
        try:
            logger.info("Upserting dim_department...")
            upsert_dataframe(df_dept, DimDepartment, session, key_cols=["department_key"])
            logger.info("dim_department completed")

            logger.info("Upserting dim_employee...")
            upsert_dataframe(df_emp, DimEmployee, session, key_cols=["employee_key"])
            logger.info("dim_employee completed")

            logger.info("Upserting dim_date...")
            upsert_dataframe(df_date, DimDate, session, key_cols=["date_id"])
            logger.info("dim_date completed")

            logger.info("Upserting fact_timesheet...")
            logger.info(f"fact_timesheet columns: {list(df_fact.columns)}")
            logger.info(f"fact_timesheet shape: {df_fact.shape}")
            upsert_dataframe(df_fact, FactTimesheet, session, key_cols=["id"])
            logger.info("fact_timesheet completed")
            
            # Step 5: Post-load Validation
            logger.info("=" * 60)
            logger.info("STEP 5: POST-LOAD VALIDATION")
            logger.info("=" * 60)
            post_load_report = validate_post_load(session, {
                "dim_department": DimDepartment,
                "dim_employee": DimEmployee,
                "dim_date": DimDate,
                "fact_timesheet": FactTimesheet
            })
            
            if not post_load_report.passed:
                logger.error("Post-load validation failed!")
            
        finally:
            session.close()
        
        logger.info("=" * 60)
        logger.info("ETL RUN COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        
        return {
            "status": "success",
            "pre_load_qc": qc_report,
            "post_load_qc": post_load_report
        }
        
    except Exception as e:
        logger.error(f"ETL FAILED: {e}")
        traceback.print_exc()
        raise

if __name__ == "__main__":
    run_etl_debug()
