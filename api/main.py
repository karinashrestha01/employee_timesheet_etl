"""FastAPI application entry point."""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.employees import router as employees_router
from api.timesheets import router as timesheets_router, employee_timesheets_router
# from api.auth_routes import router as auth_router

# Create FastAPI application
app = FastAPI(
    title="ETL Insights API",
    description="""
REST API for Employee and Timesheet management with Bearer Token Authentication.

## Authentication

### Default Users:
- **Admin**: username=`admin`, password=`admin123` 

**Change default passwords in production!**

## Features

### Authentication
- Login/Logout with bearer token
- User management (admin only)
- Token expiration (24 hours)

### Employees (CRUD)
- **GET**: List/view employees (requires auth)
- **POST/PUT/DELETE**: Modify employees (requires admin)
- Filter by active status, department, or search by name
- Pagination support

### Timesheets (Read-only)
- List timesheets with filtering (requires auth)
- Filter by date range, employee, or department
- Get individual timesheet details with employee info
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
# app.include_router(auth_router)
app.include_router(employees_router)
app.include_router(timesheets_router)
app.include_router(employee_timesheets_router)


@app.get("/", tags=["Health"])
def root():
    """API health check endpoint."""
    return {
        "status": "healthy",
        "message": "ETL Insights API is running",
        "docs": "/docs",
        "auth": "/auth/login"
    }


@app.get("/health", tags=["Health"])
def health_check():
    """Detailed health check."""
    return {
        "status": "healthy",
        "version": "1.0.0",
        "endpoints": {
            "auth": "/auth/login",
            "employees": "/employees",
            "timesheets": "/timesheets",
            "docs": "/docs",
            "redoc": "/redoc"
        }
    }


#Fast API application