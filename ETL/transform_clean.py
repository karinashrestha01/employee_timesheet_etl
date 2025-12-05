# ETL/transform_clean.py

import os, pandas as pd, logging
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def clean_nulls(df):
    df = df.replace({pd.NaT: None, pd.NA: None})
    return df.where(pd.notna(df), None)

def load_csv_files(directory, prefix, sep="|"):
    files = [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith(".csv")]
    if not files: raise ValueError(f"No `{prefix}` CSV files found in {directory}")
    df_list = []
    for file in files:
        path = os.path.join(directory, file)
        logger.info(f"Loading file: {path}")
        df = pd.read_csv(path, sep=sep, dtype=str, low_memory=False)
        df.columns = df.columns.str.strip()
        df = df.apply(lambda col: col.str.strip() if col.dtype=="object" else col)
        df_list.append(df)
    return pd.concat(df_list, ignore_index=True)

def transform_data(download_dir="datasets"):
    logger.info("===== STARTING TRANSFORMATION =====")
    today = pd.to_datetime(date.today())

    # ---- EMPLOYEE ----
    df_emp = load_csv_files(download_dir, "employee")
    df_emp.rename(columns={"client_employee_id":"employee_id", "hire_date":"hire_date",
                           "term_date":"termination_date", "job_title":"job_title",
                           "department_name":"department_name"}, inplace=True)
    df_emp["employee_id"] = df_emp["employee_id"].astype(str)
    df_emp = clean_nulls(df_emp)
    df_emp["hire_date"] = pd.to_datetime(df_emp["hire_date"], errors="coerce")
    df_emp["termination_date"] = pd.to_datetime(df_emp["termination_date"], errors="coerce")
    df_emp["is_active"] = df_emp["termination_date"].isna().astype(int)
    df_emp.fillna({"department_name":"Unknown","first_name":"","last_name":"","job_title":"Unknown"}, inplace=True)

    # ---- DEPARTMENT ----
    df_dept = df_emp[["department_id","department_name"]].drop_duplicates().reset_index(drop=True).reset_index()
    df_dept.rename(columns={"index":"department_key"}, inplace=True)
    df_dept["department_key"] += 1
    df_dept["is_active"] = 1
    df_dept["start_date"] = today
    df_dept["end_date"] = None
    df_dept = clean_nulls(df_dept)

    # ---- EMPLOYEE with department key ----
    df_emp = df_emp.merge(df_dept[["department_id","department_key"]], on="department_id", how="left")
    df_emp["employee_key"] = range(1, len(df_emp)+1)
    df_emp["start_date"] = today
    df_emp["end_date"] = None

    df_dim_employee = df_emp[["employee_key","employee_id","first_name","last_name",
                              "job_title","department_key","hire_date","termination_date",
                              "is_active","start_date","end_date"]]

    df_dim_employee = clean_nulls(df_dim_employee)

    # ---- TIMESHEET ----
    df_ts = load_csv_files(download_dir, "timesheet")
    df_ts.rename(columns={"client_employee_id":"employee_id","punch_apply_date":"work_date",
                          "punch_in_datetime":"punch_in","punch_out_datetime":"punch_out",
                          "hours_worked":"hours_worked","pay_code":"pay_code",
                          "punch_in_comment":"punch_in_comment","punch_out_comment":"punch_out_comment"}, inplace=True)
    df_ts["employee_id"] = df_ts["employee_id"].astype(str)
    df_ts = clean_nulls(df_ts)
    df_ts["work_date"] = pd.to_datetime(df_ts["work_date"], errors="coerce")
    df_ts["punch_in"] = pd.to_datetime(df_ts["punch_in"], errors="coerce")
    df_ts["punch_out"] = pd.to_datetime(df_ts["punch_out"], errors="coerce")
    df_ts["hours_worked"] = pd.to_numeric(df_ts["hours_worked"], errors="coerce").fillna(0)
    df_ts.fillna({"pay_code":"","punch_in_comment":"","punch_out_comment":""}, inplace=True)

    df_fact_timesheet = df_ts.merge(df_dim_employee[["employee_key","employee_id","department_key"]],
                                    on="employee_id", how="inner")
    df_fact_timesheet["scheduled_start"] = "09:00:00"
    df_fact_timesheet["scheduled_end"] = "17:00:00"
    
    # Only keep columns that exist in FactTimesheet model
    fact_columns = ["employee_key", "department_key", "work_date", "punch_in", "punch_out",
                    "scheduled_start", "scheduled_end", "hours_worked", "pay_code",
                    "punch_in_comment", "punch_out_comment"]
    df_fact_timesheet = df_fact_timesheet[[c for c in fact_columns if c in df_fact_timesheet.columns]]
    df_fact_timesheet = clean_nulls(df_fact_timesheet)

    # ---- DIM DATE ----
    df_dim_date = pd.DataFrame({"work_date": df_fact_timesheet["work_date"].dropna().unique()})
    df_dim_date["work_date"] = pd.to_datetime(df_dim_date["work_date"])
    df_dim_date = df_dim_date.sort_values("work_date").reset_index(drop=True)
    df_dim_date["date_id"] = df_dim_date.index + 1
    df_dim_date["year"] = df_dim_date["work_date"].dt.year
    df_dim_date["month"] = df_dim_date["work_date"].dt.month
    df_dim_date["day"] = df_dim_date["work_date"].dt.day
    df_dim_date["week"] = df_dim_date["work_date"].dt.isocalendar().week.astype(int)
    df_dim_date["quarter"] = df_dim_date["work_date"].dt.quarter
    df_dim_date = clean_nulls(df_dim_date)

    logger.info("===== TRANSFORMATION COMPLETED =====")
    return df_dim_employee, df_dept, df_dim_date, df_fact_timesheet
