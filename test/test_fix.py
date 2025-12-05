# import sys
# from pathlib import Path
# sys.path.append(str(Path(__file__).resolve().parent))

# from ETL.transform_clean import transform_data
# import logging

# logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
# logger = logging.getLogger(__name__)

# # Get transformed data
# logger.info("Running transformation...")
# df_emp, df_dept, df_date, df_fact = transform_data("datasets")

# # Check is_active column
# logger.info(f"\n'is_active' dtype: {df_dept['is_active'].dtype}")
# logger.info(f"'is_active' unique values: {df_dept['is_active'].unique()}")
# logger.info(f"'is_active' sample values: {df_dept['is_active'].head().tolist()}")

# # Verify it's integer
# if df_dept['is_active'].dtype == 'int64' or df_dept['is_active'].dtype == 'int32':
#     logger.info("✅ is_active is correctly typed as integer!")
# else:
#     logger.error(f"❌ is_active has wrong type: {df_dept['is_active'].dtype}")
