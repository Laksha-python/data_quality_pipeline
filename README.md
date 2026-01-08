## Data Quality Monitoring Pipeline

A contract-driven data quality pipeline that profiles datasets, builds historical baselines, detects anomalies, computes data quality scores, performs root-cause analysis, and triggers alerts.
Supports multiple datasets with strict isolation.

## Features

1. YAML-based data contracts
2. Dataset registry and isolation (dataset_id)
3. Automated schema generation
4. Data ingestion from CSV sources
5. Profiling (record counts, null metrics)
6. Rolling baseline construction (30-day window)
7. Drift detection:
8. Schema drift
9. Distribution drift
10. Referential drift
11. Anomaly detection and aggregation
12. Data Quality score computation
13. Root cause analysis
14. Alerting (console + mock email/Slack)
15. Dockerized execution

## Tech Stack

Python 3
PostgreSQL
pandas
psycopg2
PyYAML
Docker

## Repository Structure
DQ/
├── run_pipeline.py

├── contract_validator.py

├── contracts/

├── registry/

├── schema/

├── ingestion/

├── profiling/

├── baseline/

├── drift/

├── anomaly/

├── aggregation/

├── scoring/

├── root_cause/

├── alerting/

├── migrations/

├── data/

│   └── raw/

└── docker/

## Prerequisites

Python 3.9+

PostgreSQL

Docker (optional)

Database Setup

-- Create database and apply schema:

psql -d data_quality_db -f migrations/create_all_tables.sql

--Required schemas:
dq
raw

-- Dataset Registration

Each dataset must be registered once.

python registry/dataset_registry.py contracts/data_contract.yaml --dataset orders_pipeline

python registry/dataset_registry.py contracts/adult_income.yaml --dataset adult_income


This inserts records into dq.dq_datasets and assigns unique dataset_id.

-- Running the Pipeline (Local)

python run_pipeline.py contracts/adult_income.yaml --dataset adult_income


## Optional date controls:

python run_pipeline.py contracts/adult_income.yaml --dataset adult_income --run-date 2026-01-08

python run_pipeline.py contracts/adult_income.yaml --dataset adult_income --start-date 2026-01-01 --end-date 2026-01-07

## Running with Docker
Build image
docker build -t dq-pipeline .

## Run pipeline
docker run --rm 
-e DB_HOST=host.docker.internal 
-e DB_NAME=data_quality_db 
-e DB_USER=postgres 
-e DB_PASSWORD=your_password 
-e DB_PORT=5432 
-v C:\Users\LAKSHA\Downloads\DQ:/app 
dq-pipeline contracts/adult_income.yaml --dataset adult_income

## Pipeline Execution Order

Contract validation

Schema generation

Data ingestion

Schema drift detection

Profiling

Baseline construction

Baseline audit

Comparison engine

Distribution drift detection

Referential drift detection

Anomaly detection

Aggregation

Scoring

Root cause analysis

Alerting

Each step is logged in dq.dq_run_history.

## Key Tables

dq_datasets
dq_run_history
dq_current_stats
dq_baseline_stats
dq_anomalies
dq_aggregated_anomalies
dq_score_history
dq_root_causes

## Example Output

DQ Score computed | dataset_id=2 | score=50 | status=CRITICAL | top_issue=schema

ALERT TRIGGERED
Run Date     : 2026-01-08
DQ Score     : 50 (CRITICAL)
Top Issue    : schema

## Notes

Pipeline is idempotent.

Each dataset is fully isolated via dataset_id.

Backfills and reruns are supported.

CSV ingestion assumes files exist under data/raw/<dataset_name>/

## License
MIT
