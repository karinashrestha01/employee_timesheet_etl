## ETL Medallion Architecture Pipeline
A robust, production-ready ETL pipeline implementing the Medallion Architecture (Bronze → Silver → Gold) for processing employee and timesheet data from raw sources into a dimensional model optimized for analytics.
## Overview
This project implements a three-layer data transformation pipeline:
    -Bronze Layer: Raw data ingestion from MinIO or local files
    -Silver Layer: Data cleaning, validation, and transformation with incremental loading
    -Gold Layer: Dimensional model (star schema) for analytics and reporting

The pipeline handles employee master data and timesheet records, performing data quality checks at each stage and maintaining slowly changing dimensions (SCD Type 2).

Raw Data (MinIO/Local)
    ↓
Bronze Layer (raw schema)
    ├── raw_employee
    └── raw_timesheet
    ↓
Silver Layer (staging schema)
    ├── stg_employee
    ├── stg_timesheet
    └── etl_watermark (incremental loading)
    ↓
Gold Layer (public schema)
    ├── dim_employee
    ├── dim_department
    ├── dim_date
    └── fact_timesheet

## Features
1) Incremental Loading: Watermark-based incremental processing to handle only new records
2) Data Quality Validation: Comprehensive validation at Silver and Gold layers
3) SCD Type 2: Slowly Changing Dimensions with historical tracking
4) Idempotent Operations: Safe to re-run without data duplication
5) Flexible Data Sources: Support for MinIO object storage and local file systems
6) Comment Categorization: Standardizes timesheet comments into predefined categories
7) Referential Integrity: Ensures orphan records are filtered out
8) REST API: FastAPI-based RESTful API for CRUD operations on employees and read operations on timesheets
9) Workflow Orchestration: Apache Airflow DAGs for automated ETL pipeline execution
10) Containerized Deployment: Docker and Docker Compose for easy deployment

## Technologies Used
Python 3.8+
SQLAlchemy: ORM and database operations
Pandas: Data transformation and manipulation
PostgreSQL: Data warehouse database
MinIO: Object storage (optional)
FastAPI: REST API framework
Apache Airflow: Workflow orchestration
Docker: Containerization
Python-dotenv: Environment configuration

## Docker Deployment
The project includes Docker support for containerized deployment of all services.
Using Docker Compose

Ensure Docker and Docker Compose are installed on your system
Create a .env file with the required environment variables (see Configuration section)
Start all services:
docker-compose up -d

## Access the services:
FastAPI Swagger UI: http://localhost:8000/docs
Airflow UI: http://localhost:8080 (default credentials: admin/admin)
MinIO Console: http://localhost:9001

## API Configuration 
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
API Documentation:
Once the server is running, access the interactive documentation at:

Swagger UI: http://localhost:8000/docs
ReDoc: http://localhost:8000/redoc

Employee Endpoints:
bash# Create a new employee
curl -X POST "http://localhost:8000/employees" \
  -H "Content-Type: application/json" \
  -d '{
    "employee_id": "EMP001",
    "first_name": "John",
    "last_name": "Doe",
    "job_title": "Software Engineer",
    "department_key": 1,
    "hire_date": "2024-01-15",
    "start_date": "2024-01-15"
  }'

# List all employees with pagination
curl "http://localhost:8000/employees?page=1&page_size=20"

# Search employees by name
curl "http://localhost:8000/employees?search=John"

# Filter by active status
curl "http://localhost:8000/employees?is_active=1"

# Get specific employee
curl "http://localhost:8000/employees/1"

# Update employee
curl -X PUT "http://localhost:8000/employees/1" \
  -H "Content-Type: application/json" \
  -d '{"job_title": "Senior Software Engineer"}'

# Delete employee
curl -X DELETE "http://localhost:8000/employees/1"

## Airflow Workflow Orchestration
The project includes three Airflow DAGs for automated ETL execution:
1.Bronze Layer DAG (etl_employee_timesheet.py)
2.Silver Layer DAG (silver_layer.py)
3.Gold Layer DAG (gold_layer.py)

## Accessing Airflow UI:

Navigate to http://localhost:8080
Login with default credentials (admin/admin)
Enable the DAGs you want to run
Monitor DAG execution in the UI

Triggering DAGs Manually:
# Trigger Bronze layer
airflow dags trigger bronze_layer_etl
# Trigger Silver layer
airflow dags trigger silver_layer_etl
# Trigger Gold layer
airflow dags trigger gold_layer_etl

Airflow DAGs Reference
Bronze Layer DAG

DAG ID: bronze_layer_etl
Schedule: Daily
Tasks: Extract from MinIO → Create tables → Load employees → Load timesheets
Dependencies: None

Silver Layer DAG

DAG ID: silver_layer_etl
Schedule: Daily (after Bronze)
Tasks: Wait for Bronze → Create staging tables → Transform data → Generate summary
Dependencies: Waits for Bronze layer completion

Gold Layer DAG

DAG ID: gold_layer_etl
Schedule: Daily
Tasks: Create Gold tables → Transform dimensions → Load dimensions → Load facts
Dependencies: Requires Silver layer completion

Gold Fact Refresh DAG

DAG ID: gold_fact_refresh
Schedule: Manual trigger only
Tasks: Truncate and reload fact_timesheet table
Use Case: When transformation logic changes