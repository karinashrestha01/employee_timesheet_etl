import sys
from pathlib import Path
from typing import Optional
from datetime import date

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError, DataError
import logging

from api.database import get_db
from api.schemas import (
    EmployeeCreate, 
    EmployeeUpdate, 
    EmployeeResponse, 
    EmployeeListResponse
)
from api.auth import validate_api_key
from api.validators import validate_department_exists
from db.models import DimEmployee

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

router = APIRouter(prefix="/employees", tags=["Employees"])


@router.post("", response_model=EmployeeResponse, status_code=201)
def create_employee(
    employee: EmployeeCreate, 
    db: Session = Depends(get_db),
    _ = Depends(validate_api_key)
):
    """
    Create a new employee (admin only).
    
    - **employee_id**: Natural/business key (required)
    - **first_name**, **last_name**: Employee name
    - **job_title**: Job position
    - **department_key**: Foreign key to department
    - **hire_date**: Date employee was hired
    
    **Requires**: Admin bearer token
    """
    try:
        # Validate department exists if provided
        if employee.department_key is not None:
            validate_department_exists(db, employee.department_key)
        
        # Check if employee_id already exists
        existing = db.query(DimEmployee).filter(
            DimEmployee.employee_id == employee.employee_id
        ).first()
        if existing:
            raise HTTPException(
                status_code=400,
                detail=f"Employee with employee_id '{employee.employee_id}' already exists"
            )
        
        # Create employee
        db_employee = DimEmployee(**employee.model_dump())
        db.add(db_employee)
        db.commit()
        db.refresh(db_employee)
        
        logger.info(f"Created employee: {db_employee.employee_id} (key={db_employee.employee_key})")
        return db_employee
        
    except HTTPException:
        raise
    except IntegrityError as e:
        db.rollback()
        logger.error(f"Integrity error creating employee: {str(e)}")
        raise HTTPException(
            status_code=400,
            detail="Database integrity error. Check that all foreign keys are valid."
        )
    except DataError as e:
        db.rollback()
        logger.error(f"Data error creating employee: {str(e)}")
        raise HTTPException(
            status_code=400,
            detail="Invalid data type provided"
        )
    except Exception as e:
        db.rollback()
        logger.exception(f"Unexpected error creating employee: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while creating the employee"
        )


@router.get("", response_model=EmployeeListResponse)
def list_employees(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    is_active: Optional[int] = Query(None, description="Filter by active status (1=active, 0=historical)"),
    department_key: Optional[int] = Query(None, description="Filter by department"),
    search: Optional[str] = Query(None, description="Search by name or employee_id"),
    db: Session = Depends(get_db),
    _ = Depends(validate_api_key)
):
    """
    List all employees with pagination and filtering.
    
    - **page**: Page number (default: 1)
    - **page_size**: Items per page (default: 20, max: 100)
    - **is_active**: Filter by active status
    - **department_key**: Filter by department
    - **search**: Search in first_name, last_name, or employee_id
    
    **Requires**: Valid bearer token (user or admin)
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
def get_employee(
    employee_key: int, 
    db: Session = Depends(get_db),
    _ = Depends(validate_api_key)
):
    """
    Get a specific employee by their key.
    
    - **employee_key**: Primary key of the employee
    
    **Requires**: Valid bearer token (user or admin)
    """
    employee = db.query(DimEmployee).filter(DimEmployee.employee_key == employee_key).first()
    if not employee:
        raise HTTPException(status_code=404, detail=f"Employee with key {employee_key} not found")
    return employee


@router.put("/{employee_key}", response_model=EmployeeResponse)
def update_employee(
    employee_key: int, 
    employee_update: EmployeeUpdate, 
    db: Session = Depends(get_db),
    _ = Depends(validate_api_key)
):
    """
    Update an existing employee (admin only).
    
    - **employee_key**: Primary key of the employee to update
    - Only provided fields will be updated
    
    **Requires**: Admin bearer token
    """
    try:
        # Find employee
        employee = db.query(DimEmployee).filter(
            DimEmployee.employee_key == employee_key
        ).first()
        if not employee:
            raise HTTPException(
                status_code=404,
                detail=f"Employee with key {employee_key} not found"
            )
        
        # Get update data (only fields that were provided)
        update_data = employee_update.model_dump(exclude_unset=True)
        
        # Validate department exists if being updated
        if 'department_key' in update_data and update_data['department_key'] is not None:
            validate_department_exists(db, update_data['department_key'])
        
        # Check if employee_id is being changed to an existing one
        if 'employee_id' in update_data:
            existing = db.query(DimEmployee).filter(
                DimEmployee.employee_id == update_data['employee_id'],
                DimEmployee.employee_key != employee_key
            ).first()
            if existing:
                raise HTTPException(
                    status_code=400,
                    detail=f"Employee with employee_id '{update_data['employee_id']}' already exists"
                )
        
        # Update only provided fields
        for field, value in update_data.items():
            setattr(employee, field, value)
        
        db.commit()
        db.refresh(employee)
        
        logger.info(f"Updated employee: {employee.employee_id} (key={employee.employee_key})")
        return employee
        
    except HTTPException:
        raise
    except IntegrityError as e:
        db.rollback()
        logger.error(f"Integrity error updating employee {employee_key}: {str(e)}")
        raise HTTPException(
            status_code=400,
            detail="Database integrity error. Check that all foreign keys are valid."
        )
    except DataError as e:
        db.rollback()
        logger.error(f"Data error updating employee {employee_key}: {str(e)}")
        raise HTTPException(
            status_code=400,
            detail="Invalid data type provided"
        )
    except Exception as e:
        db.rollback()
        logger.exception(f"Unexpected error updating employee {employee_key}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while updating the employee"
        )


@router.delete("/{employee_key}", status_code=204)
def delete_employee(
    employee_key: int, 
    db: Session = Depends(get_db),
    _ = Depends(validate_api_key)
):
    """
    Delete an employee (admin only).
    
    - **employee_key**: Primary key of the employee to delete
    
    Note: This permanently removes the employee. Consider setting is_active=0 
    for soft delete to maintain historical data.
    
    **Requires**: Admin bearer token
    """
    try:
        employee = db.query(DimEmployee).filter(
            DimEmployee.employee_key == employee_key
        ).first()
        if not employee:
            raise HTTPException(
                status_code=404,
                detail=f"Employee with key {employee_key} not found"
            )
        
        employee_id = employee.employee_id
        db.delete(employee)
        db.commit()
        
        logger.info(f"Deleted employee: {employee_id} (key={employee_key})")
        return None
        
    except HTTPException:
        raise
    except IntegrityError as e:
        db.rollback()
        logger.error(f"Integrity error deleting employee {employee_key}: {str(e)}")
        raise HTTPException(
            status_code=400,
            detail="Cannot delete employee. This employee has related records (e.g., timesheets). Consider setting is_active=0 instead."
        )
    except Exception as e:
        db.rollback()
        logger.exception(f"Unexpected error deleting employee {employee_key}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while deleting the employee"
        )
