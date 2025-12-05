# api/main.py
"""FastAPI application entry point."""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.employees import router as employees_router
from api.timesheets import router as timesheets_router, employee_timesheets_router

# Create FastAPI application
app = FastAPI(
    title="ETL Insights API",
    description="""
REST API for Employee and Timesheet management.

## Features

### Employees (CRUD)
- Create, read, update, and delete employees
- Filter by active status, department, or search by name
- Pagination support

### Timesheets (Read-only)
- List timesheets with filtering by date range, employee, or department
- Get individual timesheet details with employee info
- Nested endpoint for employee-specific timesheets
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(employees_router)
app.include_router(timesheets_router)
app.include_router(employee_timesheets_router)


@app.get("/", tags=["Health"])
def root():
    """API health check endpoint."""
    return {
        "status": "healthy",
        "message": "ETL Insights API is running",
        "docs": "/docs"
    }


@app.get("/health", tags=["Health"])
def health_check():
    """Detailed health check."""
    return {
        "status": "healthy",
        "version": "1.0.0",
        "endpoints": {
            "employees": "/employees",
            "timesheets": "/timesheets",
            "docs": "/docs",
            "redoc": "/redoc"
        }
    }
