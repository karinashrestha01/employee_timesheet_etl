# api/timesheets.py
"""Timesheet read-only endpoints."""

import sys
from pathlib import Path
from typing import Optional
from datetime import date

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, joinedload

from api.database import get_db
from api.schemas import TimesheetResponse, TimesheetListResponse, TimesheetWithEmployee
from db.models import FactTimesheet, DimEmployee

router = APIRouter(prefix="/timesheets", tags=["Timesheets"])


@router.get("", response_model=TimesheetListResponse)
def list_timesheets(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    employee_key: Optional[int] = Query(None, description="Filter by employee key"),
    date_from: Optional[date] = Query(None, description="Filter by work_date >= date_from"),
    date_to: Optional[date] = Query(None, description="Filter by work_date <= date_to"),
    department_key: Optional[int] = Query(None, description="Filter by department"),
    pay_code: Optional[str] = Query(None, description="Filter by pay code"),
    db: Session = Depends(get_db)
):
    """
    List all timesheets with pagination and filtering.
    
    - **page**: Page number (default: 1)
    - **page_size**: Items per page (default: 20, max: 100)
    - **employee_key**: Filter by specific employee
    - **date_from**: Filter timesheets from this date
    - **date_to**: Filter timesheets up to this date
    - **department_key**: Filter by department
    - **pay_code**: Filter by pay code
    """
    query = db.query(FactTimesheet)
    
    # Apply filters
    if employee_key is not None:
        query = query.filter(FactTimesheet.employee_key == employee_key)
    if date_from is not None:
        query = query.filter(FactTimesheet.work_date >= date_from)
    if date_to is not None:
        query = query.filter(FactTimesheet.work_date <= date_to)
    if department_key is not None:
        query = query.filter(FactTimesheet.department_key == department_key)
    if pay_code is not None:
        query = query.filter(FactTimesheet.pay_code == pay_code)
    
    # Order by work_date descending (most recent first)
    query = query.order_by(FactTimesheet.work_date.desc())
    
    # Get total count
    total = query.count()
    
    # Apply pagination
    offset = (page - 1) * page_size
    timesheets = query.offset(offset).limit(page_size).all()
    
    return TimesheetListResponse(
        total=total,
        page=page,
        page_size=page_size,
        timesheets=timesheets
    )


@router.get("/{timesheet_id}", response_model=TimesheetWithEmployee)
def get_timesheet(timesheet_id: int, db: Session = Depends(get_db)):
    """
    Get a specific timesheet by ID with employee details.
    
    - **timesheet_id**: Primary key of the timesheet
    """
    timesheet = (
        db.query(FactTimesheet)
        .options(joinedload(FactTimesheet.employee))
        .filter(FactTimesheet.id == timesheet_id)
        .first()
    )
    if not timesheet:
        raise HTTPException(status_code=404, detail=f"Timesheet with id {timesheet_id} not found")
    return timesheet


# Nested route for employee timesheets
employee_timesheets_router = APIRouter(prefix="/employees", tags=["Employees"])


@employee_timesheets_router.get("/{employee_key}/timesheets", response_model=TimesheetListResponse)
def get_employee_timesheets(
    employee_key: int,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    date_from: Optional[date] = Query(None, description="Filter by work_date >= date_from"),
    date_to: Optional[date] = Query(None, description="Filter by work_date <= date_to"),
    db: Session = Depends(get_db)
):
    """
    Get all timesheets for a specific employee.
    
    - **employee_key**: Primary key of the employee
    - **date_from**: Filter timesheets from this date
    - **date_to**: Filter timesheets up to this date
    """
    # Verify employee exists
    employee = db.query(DimEmployee).filter(DimEmployee.employee_key == employee_key).first()
    if not employee:
        raise HTTPException(status_code=404, detail=f"Employee with key {employee_key} not found")
    
    query = db.query(FactTimesheet).filter(FactTimesheet.employee_key == employee_key)
    
    # Apply date filters
    if date_from is not None:
        query = query.filter(FactTimesheet.work_date >= date_from)
    if date_to is not None:
        query = query.filter(FactTimesheet.work_date <= date_to)
    
    # Order by work_date descending
    query = query.order_by(FactTimesheet.work_date.desc())
    
    # Get total count
    total = query.count()
    
    # Apply pagination
    offset = (page - 1) * page_size
    timesheets = query.offset(offset).limit(page_size).all()
    
    return TimesheetListResponse(
        total=total,
        page=page,
        page_size=page_size,
        timesheets=timesheets
    )
