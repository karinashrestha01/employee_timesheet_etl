# # transform.py import os import pandas as pd def transform_data(download_dir="datasets"): """ Complete ETL for employee and timesheet datasets: - Clean data - Handle missing values - Remove duplicates - Trim extra spaces - Correct data types - Create derived columns for KPIs: Active Headcount, Turnover, Average Tenure, Avg Working Hours, Late Arrival, Early Departure, Overtime, Rolling Average, Early Attrition """ # ----------------------------- # 1. Load Employee Data # ----------------------------- employee_files = [f for f in os.listdir(download_dir) if f.startswith("employee") and f.endswith(".csv")] df_employee_list = [] for file in employee_files: df = pd.read_csv(os.path.join(download_dir, file), sep='|', dtype=str, low_memory=False) df.columns = df.columns.str.strip() df.rename(columns={ 'client_employee_id': 'id', 'hire_date': 'date_joined', 'term_date': 'termination_date', 'job_title': 'role' }, inplace=True) df_employee_list.append(df) if not df_employee_list: raise ValueError("No employee files found in the directory.") df_employee = pd.concat(df_employee_list, ignore_index=True) df_employee['id'] = df_employee['id'].astype(str) # Ensure department column exists if 'department' not in df_employee.columns: df_employee['department'] = 'Unknown' # ----------------------------- # 2. Load Timesheet Data # ----------------------------- timesheet_files = [f for f in os.listdir(download_dir) if f.startswith("timesheet") and f.endswith(".csv")] df_timesheet_list = [] for file in timesheet_files: df = pd.read_csv(os.path.join(download_dir, file), sep='|', dtype=str, low_memory=False) df.columns = df.columns.str.strip() df.rename(columns={ 'client_employee_id': 'employee_id', 'punch_apply_date': 'date', 'punch_in_datetime': 'clock_in_time', 'punch_out_datetime': 'clock_out_time', 'hours_worked': 'worked_hours' }, inplace=True) df_timesheet_list.append(df) if not df_timesheet_list: raise ValueError("No timesheet files found in the directory.") df_timesheet = pd.concat(df_timesheet_list, ignore_index=True) df_timesheet['employee_id'] = df_timesheet['employee_id'].astype(str) # ----------------------------- # 3. Clean Employee Data # ----------------------------- for col in df_employee.select_dtypes(include="object").columns: df_employee[col] = df_employee[col].str.strip() df_employee['date_joined'] = pd.to_datetime(df_employee['date_joined'], errors='coerce') df_employee['termination_date'] = pd.to_datetime(df_employee['termination_date'], errors='coerce') df_employee.fillna({'department': 'Unknown', 'role': 'Unknown'}, inplace=True) df_employee = df_employee.drop_duplicates() # ----------------------------- # 4. Clean Timesheet Data # ----------------------------- for col in df_timesheet.select_dtypes(include="object").columns: df_timesheet[col] = df_timesheet[col].str.strip() df_timesheet['date'] = pd.to_datetime(df_timesheet['date'], errors='coerce') df_timesheet['clock_in_time'] = pd.to_datetime(df_timesheet['clock_in_time'], errors='coerce') df_timesheet['clock_out_time'] = pd.to_datetime(df_timesheet['clock_out_time'], errors='coerce') df_timesheet['worked_hours'] = pd.to_numeric(df_timesheet['worked_hours'], errors='coerce').fillna(0) df_timesheet = df_timesheet.drop_duplicates() # ----------------------------- # 5. Derived Columns for KPIs # ----------------------------- today = pd.Timestamp.now() # Employee tenure and early attrition df_employee['tenure_days'] = (df_employee['termination_date'].fillna(today) - df_employee['date_joined']).dt.days df_employee['early_attrition'] = ((df_employee['termination_date'] - df_employee['date_joined']).dt.days <= 90).astype(int) # Merge timesheet with employee df_timesheet = df_timesheet.merge( df_employee[['id', 'date_joined', 'termination_date']], left_on='employee_id', right_on='id', how='left' ) # Attendance KPIs shift_start = pd.to_timedelta(9, unit='h') shift_end = pd.to_timedelta(17, unit='h') grace = pd.Timedelta(minutes=5) df_timesheet['late_arrival'] = ((df_timesheet['clock_in_time'] - df_timesheet['date'].dt.normalize() - shift_start) > grace).astype(int) df_timesheet['early_departure'] = ((shift_end - (df_timesheet['clock_out_time'] - df_timesheet['date'].dt.normalize())) > grace).astype(int) df_timesheet['overtime_hours'] = (df_timesheet['worked_hours'] - 8).clip(lower=0) # Rolling average working hours per employee (7-day) df_timesheet['rolling_avg_hours'] = df_timesheet.groupby('employee_id')['worked_hours'].transform(lambda x: x.rolling(7, min_periods=1).mean()) # ----------------------------- # 6. Analytics KPIs Aggregation # ----------------------------- # Active Headcount Over Time df_employee['active_date_start'] = df_employee['date_joined'] df_employee['active_date_end'] = df_employee['termination_date'].fillna(today) # Turnover Trend (monthly terminations) df_turnover = df_employee.dropna(subset=['termination_date']).copy() df_turnover['term_month'] = df_turnover['termination_date'].dt.to_period('M') # Average Tenure by Department df_avg_tenure = df_employee.groupby('department')['tenure_days'].mean().reset_index().rename( columns={'tenure_days': 'avg_tenure_days'} ) # Average Working Hours per Employee df_avg_worked_hours = df_timesheet.groupby('employee_id')['worked_hours'].mean().reset_index().rename( columns={'worked_hours': 'avg_worked_hours'} ) # Total Overtime Count df_overtime = df_timesheet.groupby('employee_id')['overtime_hours'].sum().reset_index() # Late Arrival and Early Departure counts per employee df_late_early = df_timesheet.groupby('employee_id')[['late_arrival', 'early_departure']].sum().reset_index() # Early Attrition Rate (proportion of employees leaving within 90 days) early_attrition_rate = df_employee['early_attrition'].mean() # Combine employee-level KPIs into one DataFrame df_kpi_employee = df_employee[['id', 'department', 'role']].copy() df_kpi_employee = df_kpi_employee.merge(df_avg_worked_hours, left_on='id', right_on='employee_id', how='left') df_kpi_employee = df_kpi_employee.merge(df_overtime, left_on='id', right_on='employee_id', how='left') df_kpi_employee = df_kpi_employee.merge(df_late_early, left_on='id', right_on='employee_id', how='left') # ----------------------------- # 7. Final Schema Alignment # ----------------------------- # DimEmployee # Expected: employee_id, department, role, date_joined, termination_date, tenure_days, early_attrition df_employee = df_employee.rename(columns={'id': 'employee_id'}) df_employee = df_employee[[ 'employee_id', 'department', 'role', 'date_joined', 'termination_date', 'tenure_days', 'early_attrition' ]] # FactTimesheet # Expected: employee_id, date, clock_in_time, clock_out_time, worked_hours, # late_arrival, early_departure, overtime_hours, rolling_avg_hours # Note: 'id' is autoincrement in DB, so we don't pass it. df_timesheet = df_timesheet[[ 'employee_id', 'date', 'clock_in_time', 'clock_out_time', 'worked_hours', 'late_arrival', 'early_departure', 'overtime_hours', 'rolling_avg_hours' ]] # FactEmployeeKPI # Expected: employee_id, avg_worked_hours, total_overtime_hours, # total_late_arrival, total_early_departure df_kpi_employee = df_kpi_employee.rename(columns={ 'id': 'employee_id', 'overtime_hours': 'total_overtime_hours', 'late_arrival': 'total_late_arrival', 'early_departure': 'total_early_departure' }) df_kpi_employee = df_kpi_employee[[ 'employee_id', 'avg_worked_hours', 'total_overtime_hours', 'total_late_arrival', 'total_early_departure' ]] return df_employee, df_timesheet, df_kpi_employee, early_attrition_rate, df_avg_tenure, df_turnover # ----------------------------- # Example usage # ----------------------------- if __name__ == "__main__": download_dir = "datasets" df_employee, df_timesheet, df_kpi_employee, early_attrition_rate, df_avg_tenure, df_turnover = transform_data(download_dir) print("Employee Data Sample:") print(df_employee.head()) print("\nTimesheet Data Sample:") print(df_timesheet.head()) print("\nEmployee-Level KPIs:") print(df_kpi_employee.head()) print("\nEarly Attrition Rate:", early_attrition_rate) print("\nAverage Tenure by Department:") print(df_avg_tenure) print("\nTurnover Trend (monthly):") print(df_turnover.head())# transform.py import os import pandas as pd def transform_data(download_dir="datasets"): """ Complete ETL for employee and timesheet datasets: - Clean data - Handle missing values - Remove duplicates - Trim extra spaces - Correct data types - Create derived columns for KPIs: Active Headcount, Turnover, Average Tenure, Avg Working Hours, Late Arrival, Early Departure, Overtime, Rolling Average, Early Attrition """ # ----------------------------- # 1. Load Employee Data # ----------------------------- employee_files = [f for f in os.listdir(download_dir) if f.startswith("employee") and f.endswith(".csv")] df_employee_list = [] for file in employee_files: df = pd.read_csv(os.path.join(download_dir, file), sep='|', dtype=str, low_memory=False) df.columns = df.columns.str.strip() df.rename(columns={ 'client_employee_id': 'id', 'hire_date': 'date_joined', 'term_date': 'termination_date', 'job_title': 'role' }, inplace=True) df_employee_list.append(df) if not df_employee_list: raise ValueError("No employee files found in the directory.") df_employee = pd.concat(df_employee_list, ignore_index=True) df_employee['id'] = df_employee['id'].astype(str) # Ensure department column exists if 'department' not in df_employee.columns: df_employee['department'] = 'Unknown' # ----------------------------- # 2. Load Timesheet Data # ----------------------------- timesheet_files = [f for f in os.listdir(download_dir) if f.startswith("timesheet") and f.endswith(".csv")] df_timesheet_list = [] for file in timesheet_files: df = pd.read_csv(os.path.join(download_dir, file), sep='|', dtype=str, low_memory=False) df.columns = df.columns.str.strip() df.rename(columns={ 'client_employee_id': 'employee_id', 'punch_apply_date': 'date', 'punch_in_datetime': 'clock_in_time', 'punch_out_datetime': 'clock_out_time', 'hours_worked': 'worked_hours' }, inplace=True) df_timesheet_list.append(df) if not df_timesheet_list: raise ValueError("No timesheet files found in the directory.") df_timesheet = pd.concat(df_timesheet_list, ignore_index=True) df_timesheet['employee_id'] = df_timesheet['employee_id'].astype(str) # ----------------------------- # 3. Clean Employee Data # ----------------------------- for col in df_employee.select_dtypes(include="object").columns: df_employee[col] = df_employee[col].str.strip() df_employee['date_joined'] = pd.to_datetime(df_employee['date_joined'], errors='coerce') df_employee['termination_date'] = pd.to_datetime(df_employee['termination_date'], errors='coerce') df_employee.fillna({'department': 'Unknown', 'role': 'Unknown'}, inplace=True) df_employee = df_employee.drop_duplicates() # ----------------------------- # 4. Clean Timesheet Data # ----------------------------- for col in df_timesheet.select_dtypes(include="object").columns: df_timesheet[col] = df_timesheet[col].str.strip() df_timesheet['date'] = pd.to_datetime(df_timesheet['date'], errors='coerce') df_timesheet['clock_in_time'] = pd.to_datetime(df_timesheet['clock_in_time'], errors='coerce') df_timesheet['clock_out_time'] = pd.to_datetime(df_timesheet['clock_out_time'], errors='coerce') df_timesheet['worked_hours'] = pd.to_numeric(df_timesheet['worked_hours'], errors='coerce').fillna(0) df_timesheet = df_timesheet.drop_duplicates() # ----------------------------- # 5. Derived Columns for KPIs # ----------------------------- today = pd.Timestamp.now() # Employee tenure and early attrition df_employee['tenure_days'] = (df_employee['termination_date'].fillna(today) - df_employee['date_joined']).dt.days df_employee['early_attrition'] = ((df_employee['termination_date'] - df_employee['date_joined']).dt.days <= 90).astype(int) # Merge timesheet with employee df_timesheet = df_timesheet.merge( df_employee[['id', 'date_joined', 'termination_date']], left_on='employee_id', right_on='id', how='left' ) # Attendance KPIs shift_start = pd.to_timedelta(9, unit='h') shift_end = pd.to_timedelta(17, unit='h') grace = pd.Timedelta(minutes=5) df_timesheet['late_arrival'] = ((df_timesheet['clock_in_time'] - df_timesheet['date'].dt.normalize() - shift_start) > grace).astype(int) df_timesheet['early_departure'] = ((shift_end - (df_timesheet['clock_out_time'] - df_timesheet['date'].dt.normalize())) > grace).astype(int) df_timesheet['overtime_hours'] = (df_timesheet['worked_hours'] - 8).clip(lower=0) # Rolling average working hours per employee (7-day) df_timesheet['rolling_avg_hours'] = df_timesheet.groupby('employee_id')['worked_hours'].transform(lambda x: x.rolling(7, min_periods=1).mean()) # ----------------------------- # 6. Analytics KPIs Aggregation # ----------------------------- # Active Headcount Over Time df_employee['active_date_start'] = df_employee['date_joined'] df_employee['active_date_end'] = df_employee['termination_date'].fillna(today) # Turnover Trend (monthly terminations) df_turnover = df_employee.dropna(subset=['termination_date']).copy() df_turnover['term_month'] = df_turnover['termination_date'].dt.to_period('M') # Average Tenure by Department df_avg_tenure = df_employee.groupby('department')['tenure_days'].mean().reset_index().rename( columns={'tenure_days': 'avg_tenure_days'} ) # Average Working Hours per Employee df_avg_worked_hours = df_timesheet.groupby('employee_id')['worked_hours'].mean().reset_index().rename( columns={'worked_hours': 'avg_worked_hours'} ) # Total Overtime Count df_overtime = df_timesheet.groupby('employee_id')['overtime_hours'].sum().reset_index() # Late Arrival and Early Departure counts per employee df_late_early = df_timesheet.groupby('employee_id')[['late_arrival', 'early_departure']].sum().reset_index() # Early Attrition Rate (proportion of employees leaving within 90 days) early_attrition_rate = df_employee['early_attrition'].mean() # Combine employee-level KPIs into one DataFrame df_kpi_employee = df_employee[['id', 'department', 'role']].copy() df_kpi_employee = df_kpi_employee.merge(df_avg_worked_hours, left_on='id', right_on='employee_id', how='left') df_kpi_employee = df_kpi_employee.merge(df_overtime, left_on='id', right_on='employee_id', how='left') df_kpi_employee = df_kpi_employee.merge(df_late_early, left_on='id', right_on='employee_id', how='left') # ----------------------------- # 7. Final Schema Alignment # ----------------------------- # DimEmployee # Expected: employee_id, department, role, date_joined, termination_date, tenure_days, early_attrition df_employee = df_employee.rename(columns={'id': 'employee_id'}) df_employee = df_employee[[ 'employee_id', 'department', 'role', 'date_joined', 'termination_date', 'tenure_days', 'early_attrition' ]] # FactTimesheet # Expected: employee_id, date, clock_in_time, clock_out_time, worked_hours, # late_arrival, early_departure, overtime_hours, rolling_avg_hours # Note: 'id' is autoincrement in DB, so we don't pass it. df_timesheet = df_timesheet[[ 'employee_id', 'date', 'clock_in_time', 'clock_out_time', 'worked_hours', 'late_arrival', 'early_departure', 'overtime_hours', 'rolling_avg_hours' ]] # FactEmployeeKPI # Expected: employee_id, avg_worked_hours, total_overtime_hours, # total_late_arrival, total_early_departure df_kpi_employee = df_kpi_employee.rename(columns={ 'id': 'employee_id', 'overtime_hours': 'total_overtime_hours', 'late_arrival': 'total_late_arrival', 'early_departure': 'total_early_departure' }) df_kpi_employee = df_kpi_employee[[ 'employee_id', 'avg_worked_hours', 'total_overtime_hours', 'total_late_arrival', 'total_early_departure' ]] return df_employee, df_timesheet, df_kpi_employee, early_attrition_rate, df_avg_tenure, df_turnover # ----------------------------- # Example usage # ----------------------------- if __name__ == "__main__": download_dir = "datasets" df_employee, df_timesheet, df_kpi_employee, early_attrition_rate, df_avg_tenure, df_turnover = transform_data(download_dir) print("Employee Data Sample:") print(df_employee.head()) print("\nTimesheet Data Sample:") print(df_timesheet.head()) print("\nEmployee-Level KPIs:") print(df_kpi_employee.head()) print("\nEarly Attrition Rate:", early_attrition_rate) print("\nAverage Tenure by Department:") print(df_avg_tenure) print("\nTurnover Trend (monthly):") print(df_turnover.head())make this code pythonic, clean 

# # transform.py

# import os
# import pandas as pd

# def transform_data(download_dir="datasets"):
#     """
#     Complete ETL for employee and timesheet datasets:
#     - Clean data
#     - Handle missing values
#     - Remove duplicates
#     - Trim extra spaces
#     - Correct data types
#     - Create derived columns for KPIs:
#         Active Headcount, Turnover, Average Tenure, Avg Working Hours,
#         Late Arrival, Early Departure, Overtime, Rolling Average, Early Attrition
#     """

#     # -----------------------------
#     # 1. Load Employee Data
#     # -----------------------------
#     employee_files = [f for f in os.listdir(download_dir) if f.startswith("employee") and f.endswith(".csv")]
#     df_employee_list = []

#     for file in employee_files:
#         df = pd.read_csv(os.path.join(download_dir, file), sep='|', dtype=str, low_memory=False)
#         df.columns = df.columns.str.strip()
#         df.rename(columns={
#             'client_employee_id': 'id',
#             'hire_date': 'date_joined',
#             'term_date': 'termination_date',
#             'job_title': 'role'
#         }, inplace=True)
#         df_employee_list.append(df)

#     if not df_employee_list:
#         raise ValueError("No employee files found in the directory.")

#     df_employee = pd.concat(df_employee_list, ignore_index=True)
#     df_employee['id'] = df_employee['id'].astype(str)

#     # Ensure department column exists
#     if 'department' not in df_employee.columns:
#         df_employee['department'] = 'Unknown'

#     # -----------------------------
#     # 2. Load Timesheet Data
#     # -----------------------------
#     timesheet_files = [f for f in os.listdir(download_dir) if f.startswith("timesheet") and f.endswith(".csv")]
#     df_timesheet_list = []

#     for file in timesheet_files:
#         df = pd.read_csv(os.path.join(download_dir, file), sep='|', dtype=str, low_memory=False)
#         df.columns = df.columns.str.strip()
#         df.rename(columns={
#             'client_employee_id': 'employee_id',
#             'punch_apply_date': 'date',
#             'punch_in_datetime': 'clock_in_time',
#             'punch_out_datetime': 'clock_out_time',
#             'hours_worked': 'worked_hours'
#         }, inplace=True)
#         df_timesheet_list.append(df)

#     if not df_timesheet_list:
#         raise ValueError("No timesheet files found in the directory.")

#     df_timesheet = pd.concat(df_timesheet_list, ignore_index=True)
#     df_timesheet['employee_id'] = df_timesheet['employee_id'].astype(str)

#     # -----------------------------
#     # 3. Clean Employee Data
#     # -----------------------------
#     for col in df_employee.select_dtypes(include="object").columns:
#         df_employee[col] = df_employee[col].str.strip()

#     df_employee['date_joined'] = pd.to_datetime(df_employee['date_joined'], errors='coerce')
#     df_employee['termination_date'] = pd.to_datetime(df_employee['termination_date'], errors='coerce')
#     df_employee.fillna({'department': 'Unknown', 'role': 'Unknown'}, inplace=True)
#     df_employee = df_employee.drop_duplicates()

#     # -----------------------------
#     # 4. Clean Timesheet Data
#     # -----------------------------
#     for col in df_timesheet.select_dtypes(include="object").columns:
#         df_timesheet[col] = df_timesheet[col].str.strip()

#     df_timesheet['date'] = pd.to_datetime(df_timesheet['date'], errors='coerce')
#     df_timesheet['clock_in_time'] = pd.to_datetime(df_timesheet['clock_in_time'], errors='coerce')
#     df_timesheet['clock_out_time'] = pd.to_datetime(df_timesheet['clock_out_time'], errors='coerce')
#     df_timesheet['worked_hours'] = pd.to_numeric(df_timesheet['worked_hours'], errors='coerce').fillna(0)
#     df_timesheet = df_timesheet.drop_duplicates()

#     # -----------------------------
#     # 5. Derived Columns for KPIs
#     # -----------------------------
#     today = pd.Timestamp.now()

#     # Employee tenure and early attrition
#     df_employee['tenure_days'] = (df_employee['termination_date'].fillna(today) - df_employee['date_joined']).dt.days
#     df_employee['early_attrition'] = ((df_employee['termination_date'] - df_employee['date_joined']).dt.days <= 90).astype(int)

#     # Merge timesheet with employee
#     df_timesheet = df_timesheet.merge(
#         df_employee[['id', 'date_joined', 'termination_date']],
#         left_on='employee_id',
#         right_on='id',
#         how='left'
#     )

#     # Attendance KPIs
#     shift_start = pd.to_timedelta(9, unit='h')
#     shift_end = pd.to_timedelta(17, unit='h')
#     grace = pd.Timedelta(minutes=5)

#     df_timesheet['late_arrival'] = ((df_timesheet['clock_in_time'] - df_timesheet['date'].dt.normalize() - shift_start) > grace).astype(int)
#     df_timesheet['early_departure'] = ((shift_end - (df_timesheet['clock_out_time'] - df_timesheet['date'].dt.normalize())) > grace).astype(int)
#     df_timesheet['overtime_hours'] = (df_timesheet['worked_hours'] - 8).clip(lower=0)

#     # Rolling average working hours per employee (7-day)
#     df_timesheet['rolling_avg_hours'] = df_timesheet.groupby('employee_id')['worked_hours'].transform(lambda x: x.rolling(7, min_periods=1).mean())

#     # -----------------------------
#     # 6. Analytics KPIs Aggregation
#     # -----------------------------
#     # Active Headcount Over Time
#     df_employee['active_date_start'] = df_employee['date_joined']
#     df_employee['active_date_end'] = df_employee['termination_date'].fillna(today)

#     # Turnover Trend (monthly terminations)
#     df_turnover = df_employee.dropna(subset=['termination_date']).copy()
#     df_turnover['term_month'] = df_turnover['termination_date'].dt.to_period('M')

#     # Average Tenure by Department
#     df_avg_tenure = df_employee.groupby('department')['tenure_days'].mean().reset_index().rename(
#         columns={'tenure_days': 'avg_tenure_days'}
#     )

#     # Average Working Hours per Employee
#     df_avg_worked_hours = df_timesheet.groupby('employee_id')['worked_hours'].mean().reset_index().rename(
#         columns={'worked_hours': 'avg_worked_hours'}
#     )

#     # Total Overtime Count
#     df_overtime = df_timesheet.groupby('employee_id')['overtime_hours'].sum().reset_index()

#     # Late Arrival and Early Departure counts per employee
#     df_late_early = df_timesheet.groupby('employee_id')[['late_arrival', 'early_departure']].sum().reset_index()

#     # Early Attrition Rate (proportion of employees leaving within 90 days)
#     early_attrition_rate = df_employee['early_attrition'].mean()

#     # Combine employee-level KPIs into one DataFrame
#     df_kpi_employee = df_employee[['id', 'department', 'role']].copy()
#     df_kpi_employee = df_kpi_employee.merge(df_avg_worked_hours, left_on='id', right_on='employee_id', how='left')
#     df_kpi_employee = df_kpi_employee.merge(df_overtime, left_on='id', right_on='employee_id', how='left')
#     df_kpi_employee = df_kpi_employee.merge(df_late_early, left_on='id', right_on='employee_id', how='left')
#     # -----------------------------
#     # 7. Final Schema Alignment
#     # -----------------------------
    
#     # DimEmployee
#     # Expected: employee_id, department, role, date_joined, termination_date, tenure_days, early_attrition
#     df_employee = df_employee.rename(columns={'id': 'employee_id'})
#     df_employee = df_employee[[
#         'employee_id', 'department', 'role', 'date_joined', 
#         'termination_date', 'tenure_days', 'early_attrition'
#     ]]

#     # FactTimesheet
#     # Expected: employee_id, date, clock_in_time, clock_out_time, worked_hours, 
#     #           late_arrival, early_departure, overtime_hours, rolling_avg_hours
#     # Note: 'id' is autoincrement in DB, so we don't pass it.
#     df_timesheet = df_timesheet[[
#         'employee_id', 'date', 'clock_in_time', 'clock_out_time', 
#         'worked_hours', 'late_arrival', 'early_departure', 
#         'overtime_hours', 'rolling_avg_hours'
#     ]]

#     # FactEmployeeKPI
#     # Expected: employee_id, avg_worked_hours, total_overtime_hours, 
#     #           total_late_arrival, total_early_departure
#     df_kpi_employee = df_kpi_employee.rename(columns={
#         'id': 'employee_id',
#         'overtime_hours': 'total_overtime_hours',
#         'late_arrival': 'total_late_arrival',
#         'early_departure': 'total_early_departure'
#     })
#     df_kpi_employee = df_kpi_employee[[
#         'employee_id', 'avg_worked_hours', 'total_overtime_hours', 
#         'total_late_arrival', 'total_early_departure'
#     ]]

#     return df_employee, df_timesheet, df_kpi_employee, early_attrition_rate, df_avg_tenure, df_turnover
   

# # -----------------------------
# # Example usage
# # -----------------------------
# if __name__ == "__main__":
#     download_dir = "datasets"
#     df_employee, df_timesheet, df_kpi_employee, early_attrition_rate, df_avg_tenure, df_turnover = transform_data(download_dir)

#     print("Employee Data Sample:")
#     print(df_employee.head())

#     print("\nTimesheet Data Sample:")
#     print(df_timesheet.head())

#     print("\nEmployee-Level KPIs:")
#     print(df_kpi_employee.head())

#     print("\nEarly Attrition Rate:", early_attrition_rate)

#     print("\nAverage Tenure by Department:")
#     print(df_avg_tenure)

#     print("\nTurnover Trend (monthly):")
#     print(df_turnover.head())
