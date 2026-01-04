"""Pydantic schemas for request/response validation."""

from datetime import date, datetime
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator, model_validator


# EMPLOYEE SCHEMAS

class EmployeeBase(BaseModel):
    """Base employee schema with common fields."""
    employee_id: Optional[str] = Field(None, description="Natural/business key for employee")
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    job_title: Optional[str] = None
    department_key: Optional[int] = None
    hire_date: Optional[date] = None
    termination_date: Optional[date] = None
    is_active: Optional[int] = Field(1, description="1 = current, 0 = historical")
    
    @field_validator('is_active')
    @classmethod
    def validate_is_active(cls, v):
        if v is not None and v not in [0, 1]:
            raise ValueError('is_active must be 0 or 1')
        return v
    
    @model_validator(mode='after')
    def validate_dates(self):
        if self.hire_date and self.termination_date:
            if self.termination_date < self.hire_date:
                raise ValueError('termination_date must be after hire_date')
        return self


class EmployeeCreate(EmployeeBase):
    """Schema for creating a new employee."""
    employee_id: str = Field(..., description="Natural/business key for employee (required)")


class EmployeeUpdate(BaseModel):
    """Schema for updating an existing employee (all fields optional)."""
    employee_id: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    job_title: Optional[str] = None
    department_key: Optional[int] = None
    hire_date: Optional[date] = None
    termination_date: Optional[date] = None
    is_active: Optional[int] = None


class EmployeeResponse(EmployeeBase):
    """Schema for employee response with all fields."""
    employee_key: int

    class Config:
        from_attributes = True


class EmployeeListResponse(BaseModel):
    """Paginated list of employees."""
    total: int
    page: int
    page_size: int
    employees: List[EmployeeResponse]



# DEPARTMENT SCHEMAS (for reference)

class DepartmentResponse(BaseModel):
    """Schema for department response."""
    department_key: int
    department_id: str
    department_name: str
    is_active: Optional[int] = None

    class Config:
        from_attributes = True


# TIMESHEET SCHEMAS

class TimesheetResponse(BaseModel):
    """Schema for timesheet response."""
    id: int
    employee_key: int
    department_key: Optional[int] = None
    work_date: Optional[date] = None
    punch_in: Optional[datetime] = None
    punch_out: Optional[datetime] = None
    scheduled_start: Optional[str] = None
    scheduled_end: Optional[str] = None
    hours_worked: Optional[float] = None
    pay_code: Optional[str] = None
    punch_in_comment: Optional[str] = None
    punch_out_comment: Optional[str] = None

    class Config:
        from_attributes = True


class TimesheetListResponse(BaseModel):
    """Paginated list of timesheets."""
    total: int
    page: int
    page_size: int
    timesheets: List[TimesheetResponse]


class TimesheetWithEmployee(TimesheetResponse):
    """Timesheet with nested employee info."""
    employee: Optional[EmployeeResponse] = None
