from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.orm import declarative_base
from datetime import datetime

BronzeBase = declarative_base()


class RawEmployee(BronzeBase):
    """Raw employee data from CSV files - no transformations applied."""
    
    __tablename__ = "raw_employee"
    __table_args__ = {"schema": "raw"}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Original CSV columns (all stored as strings)
    client_employee_id = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    job_title = Column(String)
    department_id = Column(String)
    department_name = Column(String)
    hire_date = Column(String)
    term_date = Column(String, doc="Termination date")
    
    # Metadata columns
    source_file = Column(String, nullable=False)
    loaded_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<RawEmployee(id={self.id}, emp_id={self.client_employee_id}, file={self.source_file})>"


class RawTimesheet(BronzeBase):
    __tablename__ = "raw_timesheet"
    __table_args__ = {"schema": "raw"}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Original CSV columns (all stored as strings)
    client_employee_id = Column(String)
    punch_apply_date = Column(String)
    punch_in_datetime = Column(String)
    punch_out_datetime = Column(String)
    hours_worked = Column(String)
    pay_code = Column(String)
    punch_in_comment = Column(Text)
    punch_out_comment = Column(Text)
    
    # Metadata columns
    source_file = Column(String, nullable=False)
    loaded_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<RawTimesheet(id={self.id}, emp_id={self.client_employee_id}, date={self.punch_apply_date})>"
