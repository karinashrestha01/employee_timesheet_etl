# ETL/orchestrator.py
"""
Medallion Architecture ETL Pipeline Orchestrator.
Runs Bronze → Silver → Gold data flow with quality checks.
"""

import sys
import logging
import traceback
from pathlib import Path
from typing import Dict, Any

# Ensure project root is in sys.path for imports when running directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ETL.bronze import run_bronze_load
from ETL.silver import run_silver_transform
from ETL.gold import run_gold_load
from ETL.common import validate_post_load
from ETL.common.logging import configure_logging

from db.db_utils import get_session
from db.models import DimEmployee, DimDepartment, DimDate, FactTimesheet

logger = logging.getLogger(__name__)


def run_etl_medallion(
    download_dir: str = "datasets",
    skip_on_validation_fail: bool = False
) -> Dict[str, Any]:
    """
    Run the complete Medallion ETL pipeline.
    
    Pipeline Flow:
        Bronze (Raw) → Silver (Staging) → Gold (Dimensional Model)
    
    Args:
        download_dir: Directory containing source CSV files
        skip_on_validation_fail: If True, abort ETL if staging validation fails
    
    Returns:
        dict: Results from each layer
    """
    results: Dict[str, Any] = {
        "bronze": None,
        "silver": None,
        "gold": None,
        "post_load_validation": None
    }
    
    try:
        # STEP 1: BRONZE LAYER - Load Raw Data
        logger.info("")
        logger.info("=" * 70)
        logger.info("  STEP 1: BRONZE LAYER - Loading raw data")
        logger.info("=" * 70)
        
        bronze_result = run_bronze_load(download_dir)
        results["bronze"] = bronze_result
        
        logger.info(f"Bronze complete: {bronze_result['employees']} employees, "
                   f"{bronze_result['timesheets']} timesheets loaded")
        

        # STEP 2: SILVER LAYER - Transform with Incremental Loading
        logger.info("")
        logger.info("=" * 70)
        logger.info("  STEP 2: SILVER LAYER - Transforming to staging")
        logger.info("=" * 70)
        
        silver_result = run_silver_transform(validate=True)
        results["silver"] = silver_result
        
        # Check validation 
        if silver_result.get("validation"):
            validation_failed = any(
                not report.passed for report in silver_result["validation"]
            )
            if validation_failed:
                logger.warning("Silver layer validation found issues")
                if skip_on_validation_fail:
                    raise ValueError("ETL aborted due to staging validation failures")
                logger.warning("Proceeding with Gold load despite validation issues...")
        
        logger.info(f"Silver complete: {silver_result['employees']} employees, "
                   f"{silver_result['timesheets']} timesheets processed")
        
        # STEP 3: GOLD LAYER - Load Dimensional Model
        logger.info("")
        logger.info("=" * 70)
        logger.info("  STEP 3: GOLD LAYER - Loading dimensional model")
        logger.info("=" * 70)
        
        gold_result = run_gold_load()
        results["gold"] = gold_result
        
        if gold_result.get("status") == "success":
            logger.info(f"Gold complete: {gold_result['dim_department']} departments, "
                       f"{gold_result['dim_employee']} employees, "
                       f"{gold_result['dim_date']} dates, "
                       f"{gold_result['fact_timesheet']} timesheets")
        
        # STEP 4: POST-LOAD VALIDATION
        logger.info("")
        logger.info("=" * 70)
        logger.info("  STEP 4: POST-LOAD VALIDATION")
        logger.info("=" * 70)
        
        session = get_session()
        try:
            post_load_report = validate_post_load(session, {
                "dim_department": DimDepartment,
                "dim_employee": DimEmployee,
                "dim_date": DimDate,
                "fact_timesheet": FactTimesheet
            })
            results["post_load_validation"] = post_load_report
            
            if not post_load_report.passed:
                logger.error("Post-load validation failed!")
            else:
                logger.info("Post-load validation passed!")
        finally:
            session.close()
        
        logger.info("")
        logger.info("=" * 70)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info("")
        logger.info("Summary:")
        logger.info(f"  Bronze: {bronze_result['employees']} employees, "
                   f"{bronze_result['timesheets']} timesheets")
        logger.info(f"  Silver: {silver_result['employees']} employees, "
                   f"{silver_result['timesheets']} timesheets (batch: {silver_result['batch_id']})")
        if gold_result.get("status") == "success":
            logger.info(f"  Gold: {gold_result['dim_department']} depts, "
                       f"{gold_result['dim_employee']} emps, "
                       f"{gold_result['dim_date']} dates, "
                       f"{gold_result['fact_timesheet']} facts")
        logger.info("")
        
        return results
        
    except Exception as e:
        logger.error(f"ETL FAILED: {e}")
        traceback.print_exc()
        raise


# Legacy function name for backward compatibility
def run_etl_debug(download_dir: str = "datasets", skip_on_qc_fail: bool = False) -> Dict[str, Any]:
    """Legacy wrapper - calls medallion pipeline."""
    return run_etl_medallion(download_dir, skip_on_validation_fail=skip_on_qc_fail)


if __name__ == "__main__":
    configure_logging()
    run_etl_medallion()
