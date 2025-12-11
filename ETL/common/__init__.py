# ETL/common/__init__.py
"""
Common utilities shared across ETL layers.
Includes quality checks, logging, and custom exceptions.
"""

from ETL.common.quality_checks import (
    QCResult,
    QCReport,
    run_quality_checks,
    validate_post_load,
    check_row_count,
    check_nulls,
    check_duplicates,
    check_numeric_range,
    check_referential_integrity,
    check_date_range,
)
from ETL.common.exceptions import (
    ETLError,
    BronzeLoadError,
    SilverTransformError,
    GoldLoadError,
    ValidationError,
    ExtractionError,
)
from ETL.common.logging import configure_logging, get_logger

__all__ = [
    # Quality Checks
    "QCResult",
    "QCReport",
    "run_quality_checks",
    "validate_post_load",
    "check_row_count",
    "check_nulls",
    "check_duplicates",
    "check_numeric_range",
    "check_referential_integrity",
    "check_date_range",
    # Exceptions
    "ETLError",
    "BronzeLoadError",
    "SilverTransformError",
    "GoldLoadError",
    "ValidationError",
    "ExtractionError",
    # Logging
    "configure_logging",
    "get_logger",
]

