"""
Post-Processing & Quality Control Module
- Row count validation
- Null checks on critical columns
- Range/value validation
- Referential integrity checks
- Logging of validation results
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class QCResult:
    """Single quality check result."""
    check_name: str
    table_name: str
    passed: bool
    message: str
    details: Optional[Dict[str, Any]] = None


@dataclass
class QCReport:
    """Aggregated QC report for an ETL run."""
    timestamp: datetime = field(default_factory=datetime.now)
    results: List[QCResult] = field(default_factory=list)
    
    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)
    
    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if not r.passed)
    
    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.passed)
    
    def add(self, result: QCResult) -> None:
        self.results.append(result)
        status = "PASS" if result.passed else "FAIL"
        logger.info(f"[QC {status}] {result.table_name}: {result.check_name} - {result.message}")
    
    def summary(self) -> str:
        lines = [
            "=" * 60,
            f"QC REPORT - {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 60,
            f"Total Checks: {len(self.results)}",
            f"Passed: {self.passed_count}",
            f"Failed: {self.failed_count}",
            "-" * 60,
        ]
        for r in self.results:
            status = "PASS" if r.passed else "FAIL"
            lines.append(f"[{status}] {r.table_name}.{r.check_name}: {r.message}")
        lines.append("=" * 60)
        return "\n".join(lines)


# INDIVIDUAL CHECK FUNCTIONS

def check_row_count(df: pd.DataFrame, table_name: str, min_rows: int = 1) -> QCResult:
    """Check that DataFrame has minimum required rows."""
    row_count = len(df)
    passed = row_count >= min_rows
    return QCResult(
        check_name="row_count",
        table_name=table_name,
        passed=passed,
        message=f"Row count: {row_count} (min: {min_rows})",
        details={"row_count": row_count, "min_required": min_rows}
    )


def check_nulls(df: pd.DataFrame, table_name: str, critical_columns: List[str]) -> QCResult:
    """Check for null values in critical columns."""
    null_counts = {}
    for col in critical_columns:
        if col in df.columns:
            null_counts[col] = int(df[col].isna().sum())
    
    total_nulls = sum(null_counts.values())
    passed = total_nulls == 0
    
    return QCResult(
        check_name="null_check",
        table_name=table_name,
        passed=passed,
        message=f"Nulls in critical columns: {total_nulls}" + (f" ({null_counts})" if not passed else ""),
        details={"null_counts": null_counts}
    )


def check_duplicates(df: pd.DataFrame, table_name: str, key_columns: List[str]) -> QCResult:
    """Check for duplicate records based on key columns."""
    existing_cols = [c for c in key_columns if c in df.columns]
    if not existing_cols:
        return QCResult(
            check_name="duplicate_check",
            table_name=table_name,
            passed=True,
            message="No key columns found to check"
        )
    
    duplicate_count = df.duplicated(subset=existing_cols, keep=False).sum()
    passed = duplicate_count == 0
    
    return QCResult(
        check_name="duplicate_check",
        table_name=table_name,
        passed=passed,
        message=f"Duplicates on {existing_cols}: {duplicate_count}",
        details={"duplicate_count": int(duplicate_count), "key_columns": existing_cols}
    )


def check_numeric_range(
    df: pd.DataFrame, 
    table_name: str, 
    column: str, 
    min_val: Optional[float] = None, 
    max_val: Optional[float] = None
) -> QCResult:
    """Check that numeric values fall within expected range."""
    if column not in df.columns:
        return QCResult(
            check_name=f"range_check_{column}",
            table_name=table_name,
            passed=True,
            message=f"Column '{column}' not found"
        )
    
    col_data = pd.to_numeric(df[column], errors='coerce')
    issues = []
    
    if min_val is not None:
        below_min = (col_data < min_val).sum()
        if below_min > 0:
            issues.append(f"{below_min} values below {min_val}")
    
    if max_val is not None:
        above_max = (col_data > max_val).sum()
        if above_max > 0:
            issues.append(f"{above_max} values above {max_val}")
    
    passed = len(issues) == 0
    actual_min = col_data.min() if not col_data.isna().all() else None
    actual_max = col_data.max() if not col_data.isna().all() else None
    
    return QCResult(
        check_name=f"range_check_{column}",
        table_name=table_name,
        passed=passed,
        message=f"Range [{actual_min}, {actual_max}]" + (f" - Issues: {', '.join(issues)}" if issues else " OK"),
        details={"min": actual_min, "max": actual_max, "expected_min": min_val, "expected_max": max_val}
    )


def check_referential_integrity(
    child_df: pd.DataFrame,
    parent_df: pd.DataFrame,
    child_table: str,
    parent_table: str,
    child_key: str,
    parent_key: str
) -> QCResult:
    """Check that all foreign keys in child table exist in parent table."""
    if child_key not in child_df.columns or parent_key not in parent_df.columns:
        return QCResult(
            check_name=f"ref_integrity_{child_key}",
            table_name=child_table,
            passed=True,
            message="Key columns not found for check"
        )
    
    child_keys = set(child_df[child_key].dropna().unique())
    parent_keys = set(parent_df[parent_key].dropna().unique())
    
    orphans = child_keys - parent_keys
    passed = len(orphans) == 0
    
    return QCResult(
        check_name=f"ref_integrity_{child_key}",
        table_name=child_table,
        passed=passed,
        message=f"Orphan records: {len(orphans)}" + (f" (missing in {parent_table})" if orphans else ""),
        details={"orphan_count": len(orphans), "sample_orphans": list(orphans)[:10]}
    )


def check_date_range(
    df: pd.DataFrame,
    table_name: str,
    column: str,
    min_date: Optional[str] = None,
    max_date: Optional[str] = None
) -> QCResult:
    """Check that date values fall within expected range."""
    if column not in df.columns:
        return QCResult(
            check_name=f"date_range_{column}",
            table_name=table_name,
            passed=True,
            message=f"Column '{column}' not found"
        )
    
    col_data = pd.to_datetime(df[column], errors='coerce')
    issues = []
    
    if min_date:
        min_dt = pd.to_datetime(min_date)
        before_min = (col_data < min_dt).sum()
        if before_min > 0:
            issues.append(f"{before_min} dates before {min_date}")
    
    if max_date:
        max_dt = pd.to_datetime(max_date)
        after_max = (col_data > max_dt).sum()
        if after_max > 0:
            issues.append(f"{after_max} dates after {max_date}")
    
    passed = len(issues) == 0
    actual_min = col_data.min()
    actual_max = col_data.max()
    
    return QCResult(
        check_name=f"date_range_{column}",
        table_name=table_name,
        passed=passed,
        message=f"Date range [{actual_min}] to [{actual_max}]" + (f" - {', '.join(issues)}" if issues else " OK"),
        details={"min_date": str(actual_min), "max_date": str(actual_max)}
    )


# AGGREGATE VALIDATION FUNCTIONS

def run_quality_checks(
    df_employee: pd.DataFrame,
    df_department: pd.DataFrame,
    df_date: pd.DataFrame,
    df_timesheet: pd.DataFrame
) -> QCReport:
    """Run all quality checks on transformed data and return report."""
    report = QCReport()
    
    logger.info("=" * 60)
    logger.info("RUNNING QUALITY CHECKS")
    logger.info("=" * 60)
    
    # ---- DIMENSION: DEPARTMENT ----
    report.add(check_row_count(df_department, "dim_department", min_rows=1))
    report.add(check_nulls(df_department, "dim_department", ["department_id", "department_name"]))
    report.add(check_duplicates(df_department, "dim_department", ["department_key"]))
    
    # ---- DIMENSION: EMPLOYEE ----
    report.add(check_row_count(df_employee, "dim_employee", min_rows=1))
    report.add(check_nulls(df_employee, "dim_employee", ["employee_id", "employee_key"]))
    report.add(check_duplicates(df_employee, "dim_employee", ["employee_key"]))
    report.add(check_referential_integrity(
        df_employee, df_department, 
        "dim_employee", "dim_department",
        "department_key", "department_key"
    ))
    
    # ---- DIMENSION: DATE ----
    report.add(check_row_count(df_date, "dim_date", min_rows=1))
    report.add(check_nulls(df_date, "dim_date", ["work_date", "date_id"]))
    report.add(check_duplicates(df_date, "dim_date", ["date_id", "work_date"]))
    
    # ---- FACT: TIMESHEET ----
    report.add(check_row_count(df_timesheet, "fact_timesheet", min_rows=1))
    report.add(check_nulls(df_timesheet, "fact_timesheet", ["employee_key", "work_date"]))
    report.add(check_numeric_range(df_timesheet, "fact_timesheet", "hours_worked", min_val=0, max_val=24))
    report.add(check_referential_integrity(
        df_timesheet, df_employee,
        "fact_timesheet", "dim_employee",
        "employee_key", "employee_key"
    ))
    report.add(check_referential_integrity(
        df_timesheet, df_department,
        "fact_timesheet", "dim_department",
        "department_key", "department_key"
    ))
    report.add(check_date_range(df_timesheet, "fact_timesheet", "work_date", min_date="2000-01-01"))
    
    # Log summary
    logger.info(report.summary())
    
    return report


def validate_post_load(session, models_and_tables: Dict[str, Any]) -> QCReport:
    """
    Post-load validation: verify data in database matches expectations.
    Run this after upserting to confirm data integrity.
    
    Args:
        session: SQLAlchemy session
        models_and_tables: Dict mapping table names to model classes
        
    Returns:
        QCReport with validation results
    """
    from sqlalchemy import func
    
    report = QCReport()
    logger.info("=" * 60)
    logger.info("POST-LOAD VALIDATION")
    logger.info("=" * 60)
    
    for table_name, model in models_and_tables.items():
        try:
            count = session.query(func.count()).select_from(model).scalar()
            report.add(QCResult(
                check_name="post_load_count",
                table_name=table_name,
                passed=count > 0,
                message=f"Records in database: {count}",
                details={"db_row_count": count}
            ))
        except Exception as e:
            report.add(QCResult(
                check_name="post_load_count",
                table_name=table_name,
                passed=False,
                message=f"Error querying table: {str(e)}"
            ))
    
    logger.info(report.summary())
    return report
