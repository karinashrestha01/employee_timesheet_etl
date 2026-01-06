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
    middle_name = Column(String)
    last_name = Column(String)
    preferred_name = Column(String)
    job_code = Column(String)
    job_title = Column(String)
    job_start_date = Column(String)
    organization_id = Column(String)
    organization_name = Column(String)
    department_id = Column(String)
    department_name = Column(String)
    dob = Column(String)
    hire_date = Column(String)
    recent_hire_date = Column(String)
    anniversary_date = Column(String)
    term_date = Column(String)
    years_of_experience = Column(String)
    work_email = Column(String)
    address = Column(String)
    city = Column(String)
    state = Column(String)
    zip = Column(String)
    country = Column(String)
    anniversary_date = Column(String)
    manager_employee_id = Column(String)
    manager_employee_name = Column(String)
    fte_status = Column(String)
    is_per_deim = Column(String)

    cell_phone = Column(String)
    work_phone = Column(String)
    scheduled_weekly_hour = Column(String)
    active_status = Column(String, doc="Active status")
    term_date = Column(String, doc="Termination date")
    termination_reason = Column(String)
    clinical_level = Column(String)
    
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
    department_id = Column(String)
    department_name = Column(String)
    home_department_id = Column(String)
    home_department_name = Column(String)
    punch_apply_date = Column(String)
    punch_in_datetime = Column(String)
    punch_out_datetime = Column(String)
    hours_worked = Column(String)
    pay_code = Column(String)
    punch_in_comment = Column(Text)
    punch_out_comment = Column(Text)

    scheduled_start_datetime = Column(String)
    scheduled_end_datetime = Column(String)
    # Metadata columns
    source_file = Column(String, nullable=False)
    loaded_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<RawTimesheet(id={self.id}, emp_id={self.client_employee_id}, date={self.punch_apply_date})>"
    


