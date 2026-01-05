"""
Test script to verify the gold layer loader fixes.
This script tests the upsert functionality with the new composite key.
"""

import logging
import pandas as pd
from datetime import date, datetime
from db.db_utils import get_engine, get_session, upsert_dataframe
from db.models import Base, DimEmployee, DimDepartment, FactTimesheet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def test_upsert_with_composite_key():
    """Test that the upsert works correctly with the composite key."""
    
    logger.info("=" * 60)
    logger.info("TESTING GOLD LAYER UPSERT FIX")
    logger.info("=" * 60)
    
    engine = get_engine()
    session = get_session()
    
    try:
        # Create tables
        logger.info("Creating tables...")
        Base.metadata.create_all(engine)
        logger.info("✓ Tables created")
        
        # Create test department
        logger.info("\nCreating test department...")
        dept_data = pd.DataFrame([{
            "department_key": 1,
            "department_id": "DEPT001",
            "department_name": "Test Department",
            "is_active": 1,
            "start_date": date.today(),
            "end_date": pd.to_datetime("2222-12-01")
        }])
        upsert_dataframe(dept_data, DimDepartment, session, key_cols=["department_key"])
        logger.info("✓ Department created")
        
        # Create test employee
        logger.info("\nCreating test employee...")
        emp_data = pd.DataFrame([{
            "employee_key": 1,
            "employee_id": "EMP001",
            "first_name": "John",
            "last_name": "Doe",
            "job_title": "Developer",
            "department_key": 1,
            "hire_date": date.today(),
            "termination_date": None,
            "is_active": 1,
            "start_date": date.today(),
            "end_date": pd.to_datetime("2222-12-01")
        }])
        upsert_dataframe(emp_data, DimEmployee, session, key_cols=["employee_key"])
        logger.info("✓ Employee created")
        
        # Test 1: Insert new timesheet record
        logger.info("\n" + "-" * 60)
        logger.info("TEST 1: Inserting new timesheet record")
        logger.info("-" * 60)
        
        timesheet_data = pd.DataFrame([{
            "employee_key": 1,
            "department_key": 1,
            "pay_code_key": None,
            "work_date": date.today(),
            "punch_in": datetime.now().replace(hour=9, minute=0),
            "punch_out": datetime.now().replace(hour=17, minute=0),
            "scheduled_start_datetime": datetime.now().replace(hour=9, minute=0),
            "scheduled_end_datetime": datetime.now().replace(hour=17, minute=0),
            "hours_worked": 8.0,
            "hours_scheduled": 8.0,
            "hours_variance": 0.0,
            "is_late_arrival": 0,
            "is_early_departure": 0,
            "late_arrival_minutes": 0.0,
            "early_departure_minutes": 0.0,
            "punch_in_comment": None,
            "punch_out_comment": None
        }])
        
        upsert_dataframe(timesheet_data, FactTimesheet, session, key_cols=["employee_key", "work_date"])
        logger.info("✓ Timesheet inserted successfully")
        
        # Verify insert
        count = session.query(FactTimesheet).count()
        logger.info(f"✓ Total timesheets in database: {count}")
        assert count == 1, f"Expected 1 timesheet, found {count}"
        
        # Test 2: Update existing timesheet (same employee_key and work_date)
        logger.info("\n" + "-" * 60)
        logger.info("TEST 2: Updating existing timesheet (same employee_key + work_date)")
        logger.info("-" * 60)
        
        updated_timesheet = pd.DataFrame([{
            "employee_key": 1,
            "department_key": 1,
            "pay_code_key": None,
            "work_date": date.today(),
            "punch_in": datetime.now().replace(hour=9, minute=15),  # Changed: 15 min late
            "punch_out": datetime.now().replace(hour=17, minute=0),
            "scheduled_start_datetime": datetime.now().replace(hour=9, minute=0),
            "scheduled_end_datetime": datetime.now().replace(hour=17, minute=0),
            "hours_worked": 7.75,  # Changed
            "hours_scheduled": 8.0,
            "hours_variance": -0.25,  # Changed
            "is_late_arrival": 1,  # Changed
            "is_early_departure": 0,
            "late_arrival_minutes": 15.0,  # Changed
            "early_departure_minutes": 0.0,
            "punch_in_comment": "Traffic delay",  # Changed
            "punch_out_comment": None
        }])
        
        upsert_dataframe(updated_timesheet, FactTimesheet, session, key_cols=["employee_key", "work_date"])
        logger.info("✓ Timesheet updated successfully")
        
        # Verify update (should still be 1 record)
        count = session.query(FactTimesheet).count()
        logger.info(f"✓ Total timesheets in database: {count}")
        assert count == 1, f"Expected 1 timesheet after update, found {count}"
        
        # Verify the values were updated
        ts = session.query(FactTimesheet).first()
        assert ts.hours_worked == 7.75, f"Expected hours_worked=7.75, got {ts.hours_worked}"
        assert ts.is_late_arrival == 1, f"Expected is_late_arrival=1, got {ts.is_late_arrival}"
        assert ts.late_arrival_minutes == 15.0, f"Expected late_arrival_minutes=15.0, got {ts.late_arrival_minutes}"
        logger.info("✓ Values updated correctly")
        
        # Test 3: Test batch processing (simulate large dataset)
        logger.info("\n" + "-" * 60)
        logger.info("TEST 3: Testing batch processing with multiple records")
        logger.info("-" * 60)
        
        # Create 100 test records
        batch_data = []
        for i in range(2, 102):  # employee_key 2-101
            batch_data.append({
                "employee_key": i,
                "department_key": 1,
                "pay_code_key": None,
                "work_date": date.today(),
                "punch_in": datetime.now().replace(hour=9, minute=0),
                "punch_out": datetime.now().replace(hour=17, minute=0),
                "scheduled_start_datetime": datetime.now().replace(hour=9, minute=0),
                "scheduled_end_datetime": datetime.now().replace(hour=17, minute=0),
                "hours_worked": 8.0,
                "hours_scheduled": 8.0,
                "hours_variance": 0.0,
                "is_late_arrival": 0,
                "is_early_departure": 0,
                "late_arrival_minutes": 0.0,
                "early_departure_minutes": 0.0,
                "punch_in_comment": None,
                "punch_out_comment": None
            })
        
        batch_df = pd.DataFrame(batch_data)
        logger.info(f"Inserting {len(batch_df)} records in batches...")
        upsert_dataframe(batch_df, FactTimesheet, session, key_cols=["employee_key", "work_date"], batch_size=50)
        logger.info("✓ Batch insert completed")
        
        # Verify batch insert
        count = session.query(FactTimesheet).count()
        logger.info(f"✓ Total timesheets in database: {count}")
        assert count == 101, f"Expected 101 timesheets, found {count}"
        
        logger.info("\n" + "=" * 60)
        logger.info("ALL TESTS PASSED! ✓")
        logger.info("=" * 60)
        logger.info("\nThe gold layer loader fix is working correctly:")
        logger.info("  ✓ Composite key (employee_key, work_date) works")
        logger.info("  ✓ Upsert correctly updates existing records")
        logger.info("  ✓ Batch processing handles large datasets")
        logger.info("  ✓ No SQL parameter limit errors")
        
    except Exception as e:
        logger.error(f"\n❌ TEST FAILED: {str(e)}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    test_upsert_with_composite_key()
