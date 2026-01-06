from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Date,
    DateTime,
    ForeignKey,
    Index,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()
class DimEmployee(Base):

    __tablename__ = "dim_employee"

    employee_key = Column(Integer, primary_key=True, autoincrement=True)
    employee_id = Column(String, nullable=False, unique= True)  # natural/business key
    first_name = Column(String)
    last_name = Column(String)
    job_title = Column(String)
    department_key = Column(Integer, ForeignKey("dim_department.department_key"))
    hire_date = Column(Date)
    termination_date = Column(Date)
    is_active = Column(Integer) 
    # start_date = Column(Date, nullable=False)  # SCD2 start
    # end_date = Column(Date)  # SCD2 end

    timesheets = relationship("FactTimesheet", back_populates="employee")
    timesheets = relationship("FactTimesheet", back_populates="employee", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<DimEmployee(key={self.employee_key}, id={self.employee_id}, dept={self.department_key}, name={self.first_name} {self.last_name})>"


class DimDepartment(Base):
    __tablename__ = "dim_department"

    department_key = Column(Integer, primary_key=True, autoincrement=True)
    department_id = Column(String, nullable=False, unique= True)  # natural/business key
    department_name = Column(String, nullable=False)
    # is_active = Column(Integer, default=1)  
    # start_date = Column(Date, nullable=False)
    # end_date = Column(Date)

    employees = relationship("DimEmployee", backref="department")

    def __repr__(self):
        return f"<DimDepartment(key={self.department_key}, id={self.department_id}, name={self.department_name})>"


class DimPayCode(Base):
    """Pay code dimension - categorizes types of work hours (regular, overtime, PTO, etc.)"""
    __tablename__ = "dim_pay_code"

    pay_code_key = Column(Integer, primary_key=True, autoincrement=True)
    pay_code = Column(String, nullable=False, unique=True)  # natural key
    pay_code_description = Column(String)
    is_overtime = Column(Integer, default=0)
    is_pto = Column(Integer, default=0)

    timesheets = relationship("FactTimesheet", back_populates="pay_code_dim")

    def __repr__(self):
        return f"<DimPayCode(key={self.pay_code_key}, code={self.pay_code})>"


class DimDate(Base):
    __tablename__ = "dim_date"

    date_id = Column(Integer, primary_key=True, autoincrement=True)
    work_date = Column(Date, unique=True, nullable=False)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    week = Column(Integer)
    quarter = Column(Integer)

    timesheets = relationship("FactTimesheet", back_populates="date")

    def __repr__(self):
        return f"<DimDate(id={self.date_id}, date={self.work_date})>"


class FactTimesheet(Base):
    __tablename__ = "fact_timesheet"

    id = Column(Integer, primary_key=True, autoincrement=True)
    employee_key = Column(Integer, ForeignKey("dim_employee.employee_key"), nullable=False, index=True)
    department_key = Column(Integer, ForeignKey("dim_department.department_key"), nullable=True, index=True)
    pay_code_key = Column(Integer, ForeignKey("dim_pay_code.pay_code_key"), nullable=True, index=True)
    work_date = Column(Date, ForeignKey("dim_date.work_date"), index=True)
    
    # Core time metrics
    punch_in = Column(DateTime)
    punch_out = Column(DateTime)
    scheduled_start_datetime = Column(DateTime)
    scheduled_end_datetime = Column(DateTime)
    hours_worked = Column(Float)
    
    # Derived metrics for analytics
    hours_scheduled = Column(Float)  # scheduled_end - scheduled_start
    # hours_variance = Column(Float)   # hours_worked - hours_scheduled
    is_late_arrival = Column(Integer)  # 1 if punch_in > scheduled_start
    is_early_departure = Column(Integer)  # 1 if punch_out < scheduled_end
    late_arrival_minutes = Column(Float)  # minutes late
    early_departure_minutes = Column(Float)  # minutes left early
    
    # Comment categories (moved from raw text to categories)
    punch_in_comment = Column(String)
    punch_out_comment = Column(String)

    employee = relationship("DimEmployee", back_populates="timesheets")
    date = relationship("DimDate", back_populates="timesheets")
    pay_code_dim = relationship("DimPayCode", back_populates="timesheets")

    __table_args__ = (
        Index("idx_employee_workdate", "employee_key", "work_date"),
        UniqueConstraint("employee_key", "work_date", name="uq_employee_workdate"),
    )

    def __repr__(self):
        return f"<FactTimesheet(emp_key={self.employee_key}, date={self.work_date})>"

