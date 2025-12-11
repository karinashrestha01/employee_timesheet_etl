# ETL/common/exceptions.py
"""
Custom exceptions for ETL pipeline.
Provides specific error types for each layer and common error scenarios.
"""

from typing import Optional, Dict, Any


class ETLError(Exception):
    """Base exception for all ETL errors."""
    
    def __init__(
        self, 
        message: str, 
        details: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None
    ):
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.original_error = original_error
    
    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class BronzeLoadError(ETLError):
    """Exception raised during Bronze layer data loading."""
    
    def __init__(
        self, 
        message: str, 
        file_path: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.pop("details", {})
        if file_path:
            details["file_path"] = file_path
        super().__init__(message, details=details, **kwargs)


class SilverTransformError(ETLError):
    """Exception raised during Silver layer transformation."""
    
    def __init__(
        self, 
        message: str, 
        table_name: Optional[str] = None,
        batch_id: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.pop("details", {})
        if table_name:
            details["table_name"] = table_name
        if batch_id:
            details["batch_id"] = batch_id
        super().__init__(message, details=details, **kwargs)


class GoldLoadError(ETLError):
    """Exception raised during Gold layer loading."""
    
    def __init__(
        self, 
        message: str, 
        dimension_table: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.pop("details", {})
        if dimension_table:
            details["dimension_table"] = dimension_table
        super().__init__(message, details=details, **kwargs)


class ValidationError(ETLError):
    """Exception raised when data validation fails."""
    
    def __init__(
        self, 
        message: str, 
        validation_type: Optional[str] = None,
        failed_checks: Optional[int] = None,
        **kwargs
    ):
        details = kwargs.pop("details", {})
        if validation_type:
            details["validation_type"] = validation_type
        if failed_checks:
            details["failed_checks"] = failed_checks
        super().__init__(message, details=details, **kwargs)


class ExtractionError(ETLError):
    """Exception raised during data extraction from source systems."""
    
    def __init__(
        self, 
        message: str, 
        source: Optional[str] = None,
        **kwargs
    ):
        details = kwargs.pop("details", {})
        if source:
            details["source"] = source
        super().__init__(message, details=details, **kwargs)
