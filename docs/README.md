# Nhanh.vn Data Pipeline

## Architecture

This project uses **Jobs/Ops approach** with the following components:

### Components
- **Jobs**: Define workflows (`nhanh_daily_job`, `nhanh_backfill_job`)
- **Ops**: Individual operations (extract, load, validate)
- **Resources**: External systems (API, Database)
- **Schedules**: Automated triggers

### Data Flow
1. **validate_date_op**: Validate partition date
2. **setup_pipeline_op**: Initialize DLT pipeline
3. **extract_bills_op**: Extract bills from API
4. **extract_imexs_op**: Extract imexs from API  
5. **load_bills_op**: Load bills to DuckDB
6. **load_imexs_op**: Load imexs to DuckDB

## Setup

1. Copy environment file:
