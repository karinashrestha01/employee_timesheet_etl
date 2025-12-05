# etl/load.py
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.db_utils import ENGINE, create_all_tables
from transform import transform_data
from sqlalchemy.exc import SQLAlchemyError

def load_data(download_dir="datasets"):
    """
    Load transformed data into PostgreSQL tables:
    - dim_employee
    - fact_timesheet
    - fact_employee_kpi
    """
    try:
        # -----------------------------
        # 1️⃣ Transform Data
        # -----------------------------
        df_employee, df_timesheet, df_kpi_employee, _, _, _ = transform_data(download_dir)

        # -----------------------------
        # 2️⃣ Ensure tables exist
        # -----------------------------
        create_all_tables()

        # -----------------------------
        # 3️⃣ Insert Data using Pandas to_sql
        # -----------------------------
        # df_employee.rename(columns={'id': 'employee_id'}, inplace=True)  # Handled in transform.py
        df_employee.to_sql(
            name="dim_employee",
            con=ENGINE,
            if_exists="append",
            index=False
        )

        df_timesheet.to_sql(
            name="fact_timesheet",
            con=ENGINE,
            if_exists="append",
            index=False
        )

        # df_kpi_employee.rename(columns={'id': 'employee_id'}, inplace=True)  # Handled in transform.py
        df_kpi_employee.to_sql(
            name="fact_employee_kpi",
            con=ENGINE,
            if_exists="append",
            index=False
        )

        print("All data loaded successfully!")

    except SQLAlchemyError as e:
        print(f"Error inserting data into DB: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


# -----------------------------
# Script Execution
# -----------------------------
if __name__ == "__main__":
    load_data(download_dir="datasets")
