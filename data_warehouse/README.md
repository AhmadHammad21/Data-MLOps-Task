# Data Warehouse - ETL Pipeline

This project data warehous design and implementation with ETL Pipeline. The following instructions will guide you through setting up the environment, installing dependencies, and running the services.

---

## Prerequisites

- Python 3.12
- Conda or Docker (depending on your preferred method)
- Required libraries listed in `requirements.txt`
- Install mysql 
- Setup mysql workbench (optional)
---

## Setup Instructions

### 1. **Create a Python Environment**
Create a Conda environment with Python 3.12:

```bash
conda create --name de_env python=3.12 -y
conda activate de_env
```


### 2. **Install Required Libraries**
Navigate to the Directory and Install the Required Libraries

```bash
cd date_warehouse
pip install -r requirements.txt
```

## Data Warehouse

### 1. Create Database and Database Tables
1. Run this command to create a database with tables in `mysql CLI`:
```bash
source C:\Users\ahmed\OneDrive\Projects\Data-MLOps-Task\data_warehouse\queries\0_create_db_tables.sql
```

### 2. Ingest Data
1. Read, ingest data and load it into the data warehouse:
```bash
python etl_pipeline.py
```

### 3. Query and Visualize
Run the python command:
```bash
python visualize_queries.py
```

### 4.To export Data Warehouse into a Parquet Files
Run the python command:
```bash
python export_dwh.py
```