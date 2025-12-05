import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent / 'db'))

from db_utils import ENGINE
from sqlalchemy import text

# Check if data was loaded
with ENGINE.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM dim_department"))
    dept_count = result.scalar()
    
    result = conn.execute(text("SELECT COUNT(*) FROM dim_employee"))
    emp_count = result.scalar()
    
    result = conn.execute(text("SELECT COUNT(*) FROM fact_timesheet"))
    fact_count = result.scalar()
    
    print(f" ETL Verification:")
    print(f"   - dim_department: {dept_count} records")
    print(f"   - dim_employee: {emp_count} records")
    print(f"   - fact_timesheet: {fact_count} records")
    
    if dept_count > 0 and emp_count > 0 and fact_count > 0:
        print(f"\nSUCCESS! ETL completed successfully!")
    else:
        print(f"\nWARNING: Some tables are empty!")
