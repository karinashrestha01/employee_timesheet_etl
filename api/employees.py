import sys
from pathlib import Path
from typing import Optional
from datetime import date

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from api.database import get_db
from api.schemas import (
    EmployeeCreate, 
    EmployeeUpdate, 
    EmployeeResponse, 
    EmployeeListResponse
)
from db.models import DimEmployee

router = APIRouter(prefix="/employees", tags=["Employees"])


@router.post("", response_model=EmployeeResponse, status_code=201)
def create_employee(employee: EmployeeCreate, db: Session = Depends(get_db)):
    """
    Create a new employee.
    
    - **employee_id**: Natural/business key (required)
    - **first_name**, **last_name**: Employee name
    - **job_title**: Job position
    - **department_key**: Foreign key to department
    - **hire_date**: Date employee was hired
    - **start_date**: SCD2 validity start date (required)
    """
    db_employee = DimEmployee(**employee.model_dump())
    db.add(db_employee)
    db.commit()
    db.refresh(db_employee)
    return db_employee


@router.get("", response_model=EmployeeListResponse)
def list_employees(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    is_active: Optional[int] = Query(None, description="Filter by active status (1=active, 0=historical)"),
    department_key: Optional[int] = Query(None, description="Filter by department"),
    search: Optional[str] = Query(None, description="Search by name or employee_id"),
    db: Session = Depends(get_db)
):
    """
    List all employees with pagination and filtering.
    
    - **page**: Page number (default: 1)
    - **page_size**: Items per page (default: 20, max: 100)
    - **is_active**: Filter by active status
    - **department_key**: Filter by department
    - **search**: Search in first_name, last_name, or employee_id
    """
    query = db.query(DimEmployee)
    
    # Apply filters
    if is_active is not None:
        query = query.filter(DimEmployee.is_active == is_active)
    if department_key is not None:
        query = query.filter(DimEmployee.department_key == department_key)
    if search:
        search_pattern = f"%{search}%"
        query = query.filter(
            (DimEmployee.first_name.ilike(search_pattern)) |
            (DimEmployee.last_name.ilike(search_pattern)) |
            (DimEmployee.employee_id.ilike(search_pattern))
        )
    
    # Get total count
    total = query.count()
    
    # Apply pagination
    offset = (page - 1) * page_size
    employees = query.offset(offset).limit(page_size).all()
    
    return EmployeeListResponse(
        total=total,
        page=page,
        page_size=page_size,
        employees=employees
    )


@router.get("/{employee_key}", response_model=EmployeeResponse)
def get_employee(employee_key: int, db: Session = Depends(get_db)):
    """
    Get a specific employee by their key.
    
    - **employee_key**: Primary key of the employee
    """
    employee = db.query(DimEmployee).filter(DimEmployee.employee_key == employee_key).first()
    if not employee:
        raise HTTPException(status_code=404, detail=f"Employee with key {employee_key} not found")
    return employee


@router.put("/{employee_key}", response_model=EmployeeResponse)
def update_employee(
    employee_key: int, 
    employee_update: EmployeeUpdate, 
    db: Session = Depends(get_db)
):
    """
    Update an existing employee.
    
    - **employee_key**: Primary key of the employee to update
    - Only provided fields will be updated
    """
    employee = db.query(DimEmployee).filter(DimEmployee.employee_key == employee_key).first()
    if not employee:
        raise HTTPException(status_code=404, detail=f"Employee with key {employee_key} not found")
    
    print( )
    
    # Update only provided fields
    update_data = employee_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(employee, field, value)
    
    db.commit()
    db.refresh(employee)
    return employee


@router.delete("/{employee_key}", status_code=204)
def delete_employee(employee_key: int, db: Session = Depends(get_db)):
    """
    Delete an employee.
    
    - **employee_key**: Primary key of the employee to delete
    
    Note: This permanently removes the employee. Consider setting is_active=0 
    for soft delete to maintain historical data.
    """
    employee = db.query(DimEmployee).filter(DimEmployee.employee_key == employee_key).first()
    if not employee:
        raise HTTPException(status_code=404, detail=f"Employee with key {employee_key} not found")
    
    db.delete(employee)
    db.commit()
    return None
