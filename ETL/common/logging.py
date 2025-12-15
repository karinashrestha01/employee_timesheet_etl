"""
Centralized logging configuration for ETL pipeline.
Provides consistent formatting and log level management.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional


# Default log format
DEFAULT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def configure_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    log_format: str = DEFAULT_FORMAT,
    date_format: str = DEFAULT_DATE_FORMAT,
) -> None:
    """
    Configure logging for the ETL pipeline.
    
    Args:
        level: Logging level (default: INFO)
        log_file: Optional path to log file
        log_format: Log message format
        date_format: Date format in log messages
    """
    handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))
    handlers.append(console_handler)
    
    # File handler (optional)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter(log_format, date_format))
        handlers.append(file_handler)
    
    # Configure root logger
    logging.basicConfig(
        level=level,
        format=log_format,
        datefmt=date_format,
        handlers=handlers,
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    Args:
        name: Logger name (usually __name__)
    
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


def create_run_log_file(base_dir: str = "logs") -> str:
    """
    Create a timestamped log file path for an ETL run.
    
    Args:
        base_dir: Base directory for log files
    
    Returns:
        Path to the log file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = Path(base_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    return str(log_dir / f"etl_run_{timestamp}.log")
