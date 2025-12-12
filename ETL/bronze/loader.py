# ETL/bronze/loader.py

import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Set, Dict, Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from db.db_utils import get_engine, get_session
from db.models_bronze import BronzeBase, RawEmployee, RawTimesheet

logger = logging.getLogger(__name__)


# SCHEMA AND TABLE MANAGEMENT
def create_bronze_schema(engine) -> None:
    """Create the 'raw' schema if it doesn't exist."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.commit()
    logger.info("Schema 'raw' created or already exists")


def create_bronze_tables(engine) -> None:
    """Create Bronze layer tables."""
    create_bronze_schema(engine)
    BronzeBase.metadata.create_all(engine)
    logger.info("Bronze tables created successfully")


# FILE TRACKING
def get_loaded_files(session: Session, table_class) -> Set[str]:
    """Get list of files already loaded into a Bronze table."""
    result = session.query(table_class.source_file).distinct().all()
    return {row[0] for row in result}

def load_csv_to_bronze(
    file_path: str, 
    table_class, 
    session: Session, 
    batch_size: int = 1000
) -> int:
    """
    Load a single CSV file into a Bronze table.
    
    Args:
        file_path: Path to the CSV file
        table_class: SQLAlchemy model class (RawEmployee or RawTimesheet)
        session: Database session
        batch_size: Number of records per insert batch
    
    Returns:
        Number of records loaded
    
    Raises:
        BronzeLoadError: If loading fails
    """
    file_name = os.path.basename(file_path)
    logger.info(f"Loading file: {file_name}")
    
    try:
        # Read CSV with all columns as strings (no type conversion)
        # Use quotechar to properly handle quoted fields with embedded pipes
        df = pd.read_csv(
            file_path, 
            sep="|", 
            dtype=str, 
            low_memory=False, 
            quotechar='"'
        )
        df.columns = df.columns.str.strip().str.replace('"', '')
        
        # Add metadata columns
        df["source_file"] = file_name
        df["loaded_at"] = datetime.utcnow()
        
        # Convert to records and insert in batches
        records = df.to_dict(orient="records")
        total_records = len(records)
        
        for i in range(0, total_records, batch_size):
            batch = records[i:i + batch_size]
            session.bulk_insert_mappings(table_class, batch)
            session.commit()
            logger.info(f"  Inserted batch {i // batch_size + 1} ({len(batch)} records)")
        
        logger.info(f"  Total: {total_records} records loaded from {file_name}")
        return total_records
        
    except Exception as e:
        logger.error(f"Failed to load {file_name}: {e}")
        session.rollback()
        raise


def load_employees_to_bronze(
    data_dir: str = "datasets", 
    session: Optional[Session] = None, 
    engine = None
) -> int:
    """
    Load all employee CSV files into Bronze layer.
    
    Args:
        data_dir: Directory containing CSV files
        session: Database session (created if not provided)
        engine: Database engine (created if not provided)
    
    Returns:
        Total number of records loaded
    """
    if engine is None:
        engine = get_engine()
    if session is None:
        session = get_session()
    
    # Get already loaded files
    loaded_files = get_loaded_files(session, RawEmployee)
    logger.info(f"Already loaded employee files: {loaded_files}")
    
    # Find new employee CSV files
    employee_files = [
        f for f in os.listdir(data_dir) 
        if f.startswith("employee") and f.endswith(".csv")
    ]
    
    total_loaded = 0
    for file_name in employee_files:
        if file_name in loaded_files:
            logger.info(f"Skipping already loaded file: {file_name}")
            continue
        
        file_path = os.path.join(data_dir, file_name)
        count = load_csv_to_bronze(file_path, RawEmployee, session)
        total_loaded += count
    
    return total_loaded


def load_timesheets_to_bronze(
    data_dir: str = "datasets", 
    session: Optional[Session] = None, 
    engine = None
) -> int:
    """
    Load all timesheet CSV files into Bronze layer.
    
    Args:
        data_dir: Directory containing CSV files
        session: Database session (created if not provided)
        engine: Database engine (created if not provided)
    
    Returns:
        Total number of records loaded
    """
    if engine is None:
        engine = get_engine()
    if session is None:
        session = get_session()
    
    # Get already loaded files
    loaded_files = get_loaded_files(session, RawTimesheet)
    logger.info(f"Already loaded timesheet files: {loaded_files}")
    
    # Find new timesheet CSV files
    timesheet_files = [
        f for f in os.listdir(data_dir) 
        if f.startswith("timesheet") and f.endswith(".csv")
    ]
    
    total_loaded = 0
    for file_name in timesheet_files:
        if file_name in loaded_files:
            logger.info(f"Skipping already loaded file: {file_name}")
            continue
        
        file_path = os.path.join(data_dir, file_name)
        count = load_csv_to_bronze(file_path, RawTimesheet, session)
        total_loaded += count
    
    return total_loaded

# MAIN ETL FUNCTION

def run_bronze_load(data_dir: str = "datasets") -> Dict[str, Any]:
    """
    Run the complete Bronze layer ETL.
    
    Args:
        data_dir: Directory containing source CSV files
    
    Returns:
        dict: Load statistics with employee and timesheet counts
    """
    logger.info("=" * 60)
    logger.info("BRONZE LAYER: Loading raw data")
    logger.info("=" * 60)
    
    engine = get_engine()
    create_bronze_tables(engine)
    
    session = get_session()
    try:
        emp_count = load_employees_to_bronze(data_dir, session, engine)
        ts_count = load_timesheets_to_bronze(data_dir, session, engine)
        
        logger.info("=" * 60)
        logger.info(f"BRONZE LAYER COMPLETE: {emp_count} employees, {ts_count} timesheets loaded")
        logger.info("=" * 60)
        
        return {"employees": emp_count, "timesheets": ts_count}
    finally:
        session.close()


if __name__ == "__main__":
    run_bronze_load()
