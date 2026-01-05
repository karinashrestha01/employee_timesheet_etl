"""Validation utilities for API endpoints."""

from sqlalchemy.orm import Session
from fastapi import HTTPException
from db.models import DimDepartment


def validate_department_exists(db: Session, department_key: int) -> bool:
    """
    Validate that a department exists in the database.
    
    Args:
        db: Database session
        department_key: Department key to validate
        
    Returns:
        True if department exists
        
    Raises:
        HTTPException: 400 if department does not exist
    """
    if department_key is None:
        return True
    
    department = db.query(DimDepartment).filter(
        DimDepartment.department_key == department_key
    ).first()
    
    if not department:
        raise HTTPException(
            status_code=400,
            detail=f"Department with key {department_key} does not exist"
        )
    
    return True
