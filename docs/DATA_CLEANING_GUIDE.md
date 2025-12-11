# Data Cleaning & Standardization Technical Guide

> **Version**: 1.0  
> **Last Updated**: December 2024  
> **ETL Pipeline**: Medallion Architecture (Bronze → Silver → Gold)

---

## Table of Contents
1. [Overview](#overview)
2. [Types of Inconsistent Data Encountered](#types-of-inconsistent-data-encountered)
3. [Data Cleaning Functions](#data-cleaning-functions)
4. [Validation Rules](#validation-rules)
5. [Orphan Record Handling](#orphan-record-handling)
6. [Processing Assumptions](#processing-assumptions)
7. [Final Clean Dataset](#final-clean-dataset)
8. [Code Reference](#code-reference)

---

## Overview

This document explains how the ETL pipeline handles inconsistent, random, or noisy data in timesheet and employee datasets. The pipeline uses a **Medallion Architecture** with three layers:

| Layer | Schema | Purpose |
|-------|--------|---------|
| **Bronze** | `raw` | Raw data ingestion - no transformation |
| **Silver** | `staging` | Data cleaning, validation, standardization |
| **Gold** | `public` | Dimensional model ready for reporting |

---

## Types of Inconsistent Data Encountered

### 1. Null/Empty Value Variations
The source data used many different representations for null values:

```python
# ETL/silver/utils.py
NULL_PLACEHOLDERS = [
    '[NULL]', '[null]', 'NULL', 'null', 'None', 'none', 
    'N/A', 'n/a', 'NA', 'na', 'NaN', 'nan', 
    '', ' ', '  ', '-', '--', '.', 'undefined'
]
```

### 2. Random/Noisy Punch Comments
Comments contained inconsistent formats:
- `"EARLY_OUT"`, `"EARLY OUT"`, `"EARLYOUT"`, `"early out"` → All mean same thing
- `"MISSED_PUNCH"`, `"IN_CHAIN"`, `"FORGOT PUNCH"` → Various missed punch indicators
- Pipe-separated values: `"LATE_IN|MISSED_PUNCH"`
- Random text not matching any category

### 3. Orphan Employee IDs
Timesheets referenced employee IDs not in the employee master:
- 722,746 orphan records with IDs like `"999999"`, `"TEST2"`, `"CMCRN-3"`
- Random numeric strings: `"502738"`, `"14916"`, `"405270"`

### 4. Date Format Issues
- Empty strings instead of null: `""`, `" "`
- Quoted dates: `"2024-10-28"`
- Placeholder strings: `"[NULL]"`

### 5. Missing Termination Dates
- Active employees had `NULL` for `termination_date`
- SCD2 `end_date` needed sentinel values

---

## Data Cleaning Functions

### String Column Cleaning
**Location**: `ETL/silver/utils.py` → `clean_string_column()`

```python
def clean_string_column(series: pd.Series, default_value: str = None) -> pd.Series:
    # 1. Convert to string, strip whitespace and quotes
    result = series.astype(str).str.strip().str.strip('"').str.strip("'")
    
    # 2. Replace all null placeholders with pd.NA
    for placeholder in NULL_PLACEHOLDERS:
        result = result.replace(placeholder, pd.NA)
    
    # 3. Fill nulls with default if provided
    if default_value is not None:
        result = result.fillna(default_value)
    
    return result
```

### Numeric Column Cleaning
**Location**: `ETL/silver/utils.py` → `clean_numeric_column()`

```python
def clean_numeric_column(series: pd.Series, default_value: float = 0.0) -> pd.Series:
    # 1. Clean as string first to handle placeholders
    cleaned = clean_string_column(series, default_value=None)
    
    # 2. Convert to numeric (coerce errors to NaN)
    result = pd.to_numeric(cleaned, errors='coerce')
    
    # 3. Fill remaining nulls
    return result.fillna(default_value)
```

### Date Column Cleaning
**Location**: `ETL/silver/utils.py` → `clean_date_column()`

```python
def clean_date_column(series: pd.Series) -> pd.Series:
    # 1. Strip quotes from strings
    series = series.astype(str).str.strip().str.strip('"').str.strip("'")
    
    # 2. Replace empty/whitespace with NA
    series = series.replace(r'^\s*$', pd.NA, regex=True)
    series = series.replace('nan', pd.NA)
    series = series.replace('None', pd.NA)
    
    # 3. Replace [NULL] placeholders
    series = series.replace('[NULL]', pd.NA)
    series = series.replace('[null]', pd.NA)
    
    # 4. Convert to datetime (errors become NaT)
    return pd.to_datetime(series, errors="coerce")
```

### Date with Sentinel Value
**Location**: `ETL/silver/utils.py` → `clean_date_column_with_sentinel()`

For `termination_date` and SCD2 `end_date` columns:

```python
SENTINEL_END_DATE = pd.to_datetime("2222-12-31")

def clean_date_column_with_sentinel(series: pd.Series) -> pd.Series:
    result = clean_date_column(series)
    # Fill nulls with sentinel date instead of NaT
    return result.fillna(SENTINEL_END_DATE)
```

---

## Comment Categorization

### Standard Categories
**Location**: `ETL/silver/utils.py` → `STANDARD_COMMENT_CATEGORIES`

| Category | Keywords Matched |
|----------|-----------------|
| **EARLY OUT** | EARLY_OUT, EARLY OUT, LEFT_EARLY, LEFT EARLY |
| **LATE OUT** | LATE_OUT, LATE OUT, VERY_LATE_OUT |
| **LATE IN** | LATE_IN, LATE IN, ARRIVED_LATE |
| **MISSED PUNCH** | MISSED_PUNCH, IN_CHAIN, FORGOT_PUNCH, NO_PUNCH |
| **PTO** | PTO, VACATION, SICK, HOLIDAY, LEAVE, TIME_OFF |
| **UNSCHEDULED** | UNSCHEDULED, EXTRA_SHIFT, OVERTIME, OT |
| **MEAL ISSUE** | MEAL_NOT_TAKEN, NO_MEAL, MISSED_MEAL |
| **SHORT SHIFT** | SHORT_SHIFT, PARTIAL_SHIFT |
| **CANCELLED DEDUCTION** | CANCELLED_DEDUCTION, DEDUCTION_CANCELLED |
| **OTHER** | Any non-empty value not matching above |
| **NA** | Null, empty, or placeholder values |

### Categorization Logic
**Location**: `ETL/silver/utils.py` → `categorize_comment()`

```python
def categorize_comment(text) -> str:
    # 1. Handle None/NaN/NA values → return "NA"
    if text is None or pd.isna(text):
        return "NA"
    
    # 2. Normalize: uppercase, strip quotes
    text = str(text).strip().strip('"').strip("'").upper()
    
    # 3. Check null placeholders → return "NA"
    if text in ["[NULL]", "NULL", "NONE", "N/A", "NA", "NAN", "", " ", "-"]:
        return "NA"
    
    # 4. Check exact/partial matches against categories
    for category, keywords in STANDARD_COMMENT_CATEGORIES.items():
        if text in keywords or any(kw in text for kw in keywords):
            return category
    
    # 5. Handle pipe-separated values: "LATE_IN|MISSED_PUNCH"
    if "|" in text:
        # Categorize each part separately
        # Return comma-separated categories: "LATE IN, MISSED PUNCH"
    
    # 6. Fallback for unrecognized content
    return "OTHER" if text else "NA"
```

---

## Validation Rules

### Employee Validation
**Location**: `ETL/silver/validator.py` → `validate_staging_employee()`

| Check | Type | Rule |
|-------|------|------|
| Row Count | ERROR | Must be > 0 |
| Null Employee ID | ERROR | No nulls allowed |
| Valid is_active | WARNING | Must be 0 or 1 |
| Duplicate Employee ID | WARNING | No duplicates |

### Timesheet Validation
**Location**: `ETL/silver/validator.py` → `validate_staging_timesheet()`

| Check | Type | Rule |
|-------|------|------|
| Row Count | ERROR | Must be > 0 |
| Null Employee ID | ERROR | No nulls allowed |
| Null Work Date | ERROR | No nulls allowed |
| Hours Worked Range | WARNING | 0 ≤ hours ≤ 24 |

### Referential Integrity
**Location**: `ETL/silver/validator.py` → `validate_staging_referential_integrity()`

| Check | Type | Rule |
|-------|------|------|
| Orphan Timesheet Records | WARNING | All employee_ids must exist in employee table |
| Employees Without Timesheets | INFO | Informational only |

---

## Orphan Record Handling

### Problem
Source timesheet data contained employee IDs that didn't exist in the employee master file:
- ~10,000+ distinct employee IDs in timesheets
- Only 50 employees in employee master
- 722,746 orphan timesheet records

### Solution
**Location**: `ETL/silver/transformer.py` → `run_silver_transform()`

```python
# Filter out orphan timesheets during Silver layer processing
if not ts_df.empty:
    # Get all valid employee_ids from staging
    all_emp_ids = pd.read_sql(
        "SELECT DISTINCT employee_id FROM staging.stg_employee", 
        engine
    )["employee_id"].tolist()
    
    original_count = len(ts_df)
    ts_df = ts_df[ts_df["employee_id"].isin(all_emp_ids)]
    filtered_count = original_count - len(ts_df)
    
    if filtered_count > 0:
        logger.warning(f"Filtered out {filtered_count} orphan timesheet records")
```

---

## Processing Assumptions

### 1. Null Value Handling
- Empty strings, whitespace, and placeholder values are treated as `NULL`
- Default values are applied where business logic requires (e.g., `hours_worked = 0.0`)

### 2. Date Handling
- Invalid date formats are coerced to `NaT` (Not a Time)
- Active employees: `termination_date = 2222-12-31` (sentinel)
- Current SCD2 records: `end_date = 2222-12-31` (sentinel)

### 3. Comment Standardization
- Comments are categorized to one of 10 standard values
- Unrecognized comments → `"OTHER"`
- Empty/null comments → `"NA"`

### 4. Employee ID Integrity
- Only timesheets with valid employee IDs (existing in employee table) are loaded
- Orphan records are logged but excluded from downstream processing

### 5. Primary Keys
- `dim_date.date_id`: Auto-generated by database (not manually assigned)
- `dim_employee.employee_key`: Sequential surrogate key
- `dim_department.department_key`: Sequential surrogate key

---

## Final Clean Dataset

After Silver layer processing, the data is ready for Gold layer transformation:

### Staging Employee (`staging.stg_employee`)
| Column | Cleaning Applied |
|--------|-----------------|
| `employee_id` | String cleaned, nulls → "UNKNOWN" |
| `first_name` | String cleaned, nulls → "" |
| `last_name` | String cleaned, nulls → "" |
| `job_title` | String cleaned, nulls → "Unknown" |
| `department_id` | String cleaned |
| `department_name` | String cleaned, nulls → "Unknown" |
| `hire_date` | Date cleaned |
| `termination_date` | Date cleaned with sentinel (2222-12-31) |
| `is_active` | Calculated: 1 if termination_date = sentinel, else 0 |

### Staging Timesheet (`staging.stg_timesheet`)
| Column | Cleaning Applied |
|--------|-----------------|
| `employee_id` | String cleaned, orphans filtered out |
| `work_date` | Date cleaned |
| `punch_in` | DateTime cleaned |
| `punch_out` | DateTime cleaned |
| `hours_worked` | Numeric cleaned, default 0.0 |
| `pay_code` | String cleaned |
| `punch_in_comment` | Categorized to standard values |
| `punch_out_comment` | Categorized to standard values |

### Gold Layer Tables
The clean staging data is transformed into dimensional model:
- `dim_department`: Unique departments with SCD2 support
- `dim_employee`: Employees with department_key foreign key
- `dim_date`: Calendar dimension from work dates
- `fact_timesheet`: Fact table with surrogate key references

---

## Code Reference

| File | Purpose |
|------|---------|
| `ETL/silver/utils.py` | Cleaning utility functions |
| `ETL/silver/transformer.py` | Silver layer ETL with cleaning logic |
| `ETL/silver/validator.py` | Validation rules and reporting |
| `ETL/gold/loader.py` | Gold layer transformation |
| `ETL/common/quality_checks.py` | Post-load quality checks |

---

## Quick Reference: Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCE DATA                               │
│  • Random employee IDs • Noisy comments • NULL placeholders     │
│  • Inconsistent dates  • Orphan references • Empty strings      │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (raw schema)                     │
│  • Raw ingestion - no transformation                            │
│  • Tracks source file and load timestamp                        │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (staging schema)                  │
│  ✓ Clean null placeholders → NULL or default                   │
│  ✓ Standardize comments → 10 categories                        │
│  ✓ Filter orphan timesheets (employee_id not in employees)     │
│  ✓ Apply sentinel dates for SCD2 columns                       │
│  ✓ Validate data quality with pass/fail checks                 │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (public schema)                    │
│  • Dimensional star schema                                      │
│  • Surrogate keys for all dimensions                            │
│  • Ready for BI reporting and analytics                         │
└─────────────────────────────────────────────────────────────────┘
```
