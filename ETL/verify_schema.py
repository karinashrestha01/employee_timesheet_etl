# # verify_schema.py
# # Script to check data integrity before loading

# import sys
# from pathlib import Path
# sys.path.append(str(Path(__file__).resolve().parent.parent))

# from ETL.transform_clean import transform_data
# import pandas as pd

# print("===== DATA INTEGRITY CHECK =====\n")

# # Load transformed data
# df_dim_employee, df_dim_department, df_dim_date, df_fact_timesheet = transform_data("datasets")

# # Check for employee IDs in fact_timesheet that don't exist in dim_employee
# fact_employee_ids = set(df_fact_timesheet['employee_id'].dropna())
# dim_employee_ids = set(df_dim_employee['employee_id'].dropna())

# missing_employees = fact_employee_ids - dim_employee_ids

# print(f"Employee IDs in fact_timesheet: {len(fact_employee_ids)}")
# print(f"Employee IDs in dim_employee: {len(dim_employee_ids)}")
# print(f"Missing employee IDs: {len(missing_employees)}")

# if missing_employees:
#     print(f"\nWARNING: The following employee IDs exist in timesheet data but NOT in employee data:")
#     sorted_missing = sorted(list(missing_employees))
#     print(sorted_missing[:20])  # Show first 20
#     if len(sorted_missing) > 20:
#         print(f"... and {len(sorted_missing) - 20} more")
    
#     # Show sample timesheet records for missing employees
#     print("\nSample timesheet records for missing employees:")
#     sample_missing = df_fact_timesheet[df_fact_timesheet['employee_id'].isin(list(missing_employees)[:5])]
#     print(sample_missing[['employee_id', 'work_date', 'hours_worked', 'pay_code']].head(10))
# else:
#     print("\n All employee IDs in fact_timesheet exist in dim_employee")

# # Check duplicate employees
# duplicate_employees = df_dim_employee[df_dim_employee.duplicated(subset=['employee_id'], keep=False)]
# if not duplicate_employees.empty:
#     print(f"\n WARNING: {len(duplicate_employees)} duplicate employee_id entries in dim_employee:")
#     print(duplicate_employees[['employee_id', 'first_name', 'last_name', 'department_key']].head(10))
# else:
#     print("\n No duplicate employee_id entries in dim_employee")

# print("\n===== CHECK COMPLETE =====")
