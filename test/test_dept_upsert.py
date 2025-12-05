# import sys
# from pathlib import Path
# sys.path.append(str(Path(__file__).resolve().parent))

# from db.db_utils import get_session, upsert_dataframe
# from ETL.transform_clean import transform_data
# from db.models import DimDepartment
# import logging

# logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
# logger = logging.getLogger(__name__)

# # Get transformed data
# logger.info("Running transformation...")
# df_emp, df_dept, df_date, df_fact = transform_data("datasets")

# logger.info(f"\nDepartment DataFrame shape: {df_dept.shape}")
# logger.info(f"\nDepartment DataFrame columns: {list(df_dept.columns)}")
# logger.info(f"\nDepartment DataFrame head:\n{df_dept.head()}")
# logger.info(f"\nDepartment DataFrame dtypes:\n{df_dept.dtypes}")

# # Check for is_active column
# if 'is_active' in df_dept.columns:
#     logger.info(f"\n'is_active' column unique values: {df_dept['is_active'].unique()}")
#     logger.info(f"'is_active' column dtype: {df_dept['is_active'].dtype}")

# # Try upserting
# session = get_session()
# try:
#     logger.info("\nAttempting upsert...")
#     upsert_dataframe(df_dept, DimDepartment, session, key_cols=["department_key"])
#     logger.info("Upsert successful!")
# except Exception as e:
#     logger.error(f" Upsert failed: {e}")
#     import traceback
#     traceback.print_exc()
# finally:
#     session.close()
