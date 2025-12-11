# db/models_silver.py
"""
Silver Layer Models - Cleaned and validated staging data.
Schema: staging
"""

from sqlalchemy import Column, Integer, String, DateTime, Date, Float, Text
from sqlalchemy.orm import declarative_base
from datetime import datetime

SilverBase = declarative_base()


class StagingEmployee(SilverBase):
    """Staging employee data - cleaned and validated."""
    
    __tablename__ = "stg_employee"
    __table_args__ = {"schema": "staging"}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Cleaned business columns
    employee_id = Column(String, nullable=False)
    first_name = Column(String)
    last_name = Column(String)
    job_title = Column(String)
    department_id = Column(String)
    department_name = Column(String)
    hire_date = Column(Date)
    termination_date = Column(Date)
    is_active = Column(Integer)
    
    # ETL metadata
    source_file = Column(String)
    bronze_loaded_at = Column(DateTime)
    etl_batch_id = Column(String, nullable=False)
    processed_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<StagingEmployee(id={self.id}, emp_id={self.employee_id})>"


class StagingTimesheet(SilverBase):
    """Staging timesheet data - cleaned and validated."""
    
    __tablename__ = "stg_timesheet"
    __table_args__ = {"schema": "staging"}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Cleaned business columns
    employee_id = Column(String, nullable=False)
    work_date = Column(Date)
    punch_in = Column(DateTime)
    punch_out = Column(DateTime)
    hours_worked = Column(Float)
    pay_code = Column(String)
    punch_in_comment = Column(Text)
    punch_out_comment = Column(Text)
    
    # ETL metadata
    source_file = Column(String)
    bronze_loaded_at = Column(DateTime)
    etl_batch_id = Column(String, nullable=False)
    processed_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<StagingTimesheet(id={self.id}, emp_id={self.employee_id}, date={self.work_date})>"


class ETLWatermark(SilverBase):
    """Track last processed timestamp for incremental loading."""
    
    __tablename__ = "etl_watermark"
    __table_args__ = {"schema": "staging"}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String, nullable=False, unique=True)
    last_processed_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<ETLWatermark(table={self.table_name}, last={self.last_processed_at})>"
