"""
scripts Pipeline for Medallion Architecture.

This package implements a three-layer data pipeline:
- Bronze: Raw data ingestion from source systems
- Silver: Data cleaning, validation, and transformation
- Gold: Dimensional model for analytics and reporting

Usage:
    from scripts import run_etl_medallion
    results = run_etl_medallion()
    
    # Or run individual layers:
    from scripts.bronze import run_bronze_load
    from scripts.silver import run_silver_transform
    from scripts.gold import run_gold_load
"""

__version__ = "1.0.0"
__author__ = "scripts Team"

# Main entry point
# from scripts.orchestrator import run_etl_medallion, run_etl_debug

# Layer-specific exports
from scripts.bronze import run_bronze_load, extract_from_minio
# from scripts.silver import run_silver_transform, run_silver_validation
# from scripts.gold import run_gold_load, refresh_fact_timesheet
# from scripts.common import validate_post_load, QCReport, QCResult

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
