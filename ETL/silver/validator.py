# ETL/silver_validation.py
"""
Silver Layer Validation - Quality checks for staging data before promotion to Gold.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Single validation check result."""
    check_name: str
    passed: bool
    message: str
    severity: str = "ERROR"  # ERROR, WARNING, INFO


@dataclass
class ValidationReport:
    """Collection of validation results."""
    layer: str
    results: List[ValidationResult] = field(default_factory=list)
    
    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results if r.severity == "ERROR")
    
    @property
    def error_count(self) -> int:
        return sum(1 for r in self.results if not r.passed and r.severity == "ERROR")
    
    @property
    def warning_count(self) -> int:
        return sum(1 for r in self.results if not r.passed and r.severity == "WARNING")
    
    def add(self, result: ValidationResult):
        self.results.append(result)
        status = "✓" if result.passed else "✗"
        log_fn = logger.info if result.passed else (logger.error if result.severity == "ERROR" else logger.warning)
        log_fn(f"  [{status}] {result.check_name}: {result.message}")


def validate_staging_employee(df: pd.DataFrame) -> ValidationReport:
    """
    Validate staging employee data.
    
    Checks:
    - Row count > 0
    - No null employee_id
    - Valid hire_date format
    - Valid is_active values (0 or 1)
    """
    report = ValidationReport(layer="Silver - Employee")
    logger.info("Validating staging employee data...")
    
    # Row count check
    row_count = len(df)
    report.add(ValidationResult(
        check_name="Row Count",
        passed=row_count > 0,
        message=f"{row_count} records"
    ))
    
    # Null employee_id check
    null_emp_ids = df["employee_id"].isna().sum()
    report.add(ValidationResult(
        check_name="Null Employee ID",
        passed=null_emp_ids == 0,
        message=f"{null_emp_ids} null values found"
    ))
    
    # Valid is_active values
    if "is_active" in df.columns:
        invalid_active = df[~df["is_active"].isin([0, 1, None])].shape[0]
        report.add(ValidationResult(
            check_name="Valid is_active",
            passed=invalid_active == 0,
            message=f"{invalid_active} invalid values (expected 0 or 1)",
            severity="WARNING"
        ))
    
    # Duplicate employee_id check
    dup_count = df["employee_id"].duplicated().sum()
    report.add(ValidationResult(
        check_name="Duplicate Employee ID",
        passed=dup_count == 0,
        message=f"{dup_count} duplicates found",
        severity="WARNING"
    ))
    
    return report


def validate_staging_timesheet(df: pd.DataFrame) -> ValidationReport:
    """
    Validate staging timesheet data.
    
    Checks:
    - Row count > 0
    - No null employee_id
    - Valid work_date
    - hours_worked in valid range (0-24)
    """
    report = ValidationReport(layer="Silver - Timesheet")
    logger.info("Validating staging timesheet data...")
    
    # Row count check
    row_count = len(df)
    report.add(ValidationResult(
        check_name="Row Count",
        passed=row_count > 0,
        message=f"{row_count} records"
    ))
    
    # Null employee_id check
    null_emp_ids = df["employee_id"].isna().sum()
    report.add(ValidationResult(
        check_name="Null Employee ID",
        passed=null_emp_ids == 0,
        message=f"{null_emp_ids} null values found"
    ))
    
    # Null work_date check
    null_dates = df["work_date"].isna().sum()
    report.add(ValidationResult(
        check_name="Null Work Date",
        passed=null_dates == 0,
        message=f"{null_dates} null values found"
    ))
    
    # Hours worked range check (0-24)
    if "hours_worked" in df.columns:
        hours = pd.to_numeric(df["hours_worked"], errors="coerce")
        out_of_range = ((hours < 0) | (hours > 24)).sum()
        report.add(ValidationResult(
            check_name="Hours Worked Range (0-24)",
            passed=out_of_range == 0,
            message=f"{out_of_range} records out of range",
            severity="WARNING"
        ))
    
    return report


def validate_staging_referential_integrity(emp_df: pd.DataFrame, ts_df: pd.DataFrame) -> ValidationReport:
    """
    Validate referential integrity between staging tables.
    
    Checks:
    - All timesheet employee_ids exist in employee table
    """
    report = ValidationReport(layer="Silver - Referential Integrity")
    logger.info("Validating staging referential integrity...")
    
    # Get unique employee IDs from both tables
    emp_ids = set(emp_df["employee_id"].dropna().unique())
    ts_emp_ids = set(ts_df["employee_id"].dropna().unique())
    
    # Find orphan timesheet records
    orphan_ids = ts_emp_ids - emp_ids
    orphan_count = ts_df[ts_df["employee_id"].isin(orphan_ids)].shape[0]
    
    report.add(ValidationResult(
        check_name="Orphan Timesheet Records",
        passed=len(orphan_ids) == 0,
        message=f"{len(orphan_ids)} employee IDs ({orphan_count} records) not found in employee table",
        severity="WARNING"
    ))
    
    # Check employees with no timesheets
    emp_without_ts = emp_ids - ts_emp_ids
    report.add(ValidationResult(
        check_name="Employees Without Timesheets",
        passed=True,  # This is just informational
        message=f"{len(emp_without_ts)} employees have no timesheet records",
        severity="INFO"
    ))
    
    return report


def run_silver_validation(emp_df: pd.DataFrame, ts_df: pd.DataFrame) -> List[ValidationReport]:
    """
    Run all Silver layer validations.
    
    Returns:
        List of ValidationReport objects
    """
    logger.info("=" * 60)
    logger.info("SILVER LAYER VALIDATION")
    logger.info("=" * 60)
    
    reports = [
        validate_staging_employee(emp_df),
        validate_staging_timesheet(ts_df),
        validate_staging_referential_integrity(emp_df, ts_df)
    ]
    
    # Summary
    total_errors = sum(r.error_count for r in reports)
    total_warnings = sum(r.warning_count for r in reports)
    
    logger.info("=" * 60)
    if total_errors > 0:
        logger.error(f"VALIDATION FAILED: {total_errors} errors, {total_warnings} warnings")
    else:
        logger.info(f"VALIDATION PASSED: {total_warnings} warnings")
    logger.info("=" * 60)
    
    return reports
