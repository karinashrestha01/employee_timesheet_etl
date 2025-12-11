# ETL/__init__.py
"""
ETL Pipeline for Medallion Architecture.

This package implements a three-layer data pipeline:
- Bronze: Raw data ingestion from source systems
- Silver: Data cleaning, validation, and transformation
- Gold: Dimensional model for analytics and reporting

Usage:
    from ETL import run_etl_medallion
    results = run_etl_medallion()
    
    # Or run individual layers:
    from ETL.bronze import run_bronze_load
    from ETL.silver import run_silver_transform
    from ETL.gold import run_gold_load
"""

__version__ = "1.0.0"
__author__ = "ETL Team"

# Main entry point
from ETL.orchestrator import run_etl_medallion, run_etl_debug

# Layer-specific exports
from ETL.bronze import run_bronze_load, extract_from_minio
from ETL.silver import run_silver_transform, run_silver_validation
from ETL.gold import run_gold_load, refresh_fact_timesheet
from ETL.common import validate_post_load, QCReport, QCResult

__all__ = [
    # Version
    "__version__",
    # Main orchestrator
    "run_etl_medallion",
    "run_etl_debug",
    # Bronze layer
    "run_bronze_load",
    "extract_from_minio",
    # Silver layer
    "run_silver_transform",
    "run_silver_validation",
    # Gold layer
    "run_gold_load",
    "refresh_fact_timesheet",
    # Common utilities
    "validate_post_load",
    "QCReport",
    "QCResult",
]
