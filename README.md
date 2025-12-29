# Data Quality Baseline System

A production-oriented data quality system that profiles raw datasets, constructs rolling baselines, and validates them through human-audited sanity checks before enforcing automated alerts.

This project emphasizes explainability, separation of concerns, and realistic tradeoffs commonly seen in large-scale data engineering environments.

---

## Architecture Overview

Raw CSV Data
↓
Ingestion Layer
↓
Profiling Engine
↓
Baseline Construction
↓
Baseline Audit (Human Validation)


Each stage is intentionally isolated to ensure modularity and debuggability.

---

## Folder Structure

dq/
├── baseline/
├── profiling/
├── ingestion/
├── data/
│ └── raw/
├── contracts/
├── README.md
└── requirements.txt


---

## Data Ingestion

Raw datasets are ingested from CSV files into PostgreSQL using a dedicated ingestion script and schema definition.

The ingestion layer is intentionally isolated from downstream logic to prevent coupling data loading with quality evaluation.

---

## Profiling Engine

The profiling engine computes daily data quality metrics such as:
- record count
- null count
- null rate
- basic numeric statistics (when applicable)

These metrics form the foundation for baseline construction.

---

## Week 1 — Baseline Intelligence

Week 1 focuses on building and validating data quality baselines.

### Implemented
- Daily profiling of raw tables
- Rolling baseline construction using a 30-day window
- Per-column baseline metrics for volume and completeness
- Read-only baseline audit for human sanity validation
- Design documentation outlining architectural decisions and tradeoffs

### Outcome
A validated and explainable baseline layer that is ready for threshold definition and automated alerting.

---

## How to Run

1. Create tables and schemas
```bash
psql -f ingestion/ddl.sql
```

2. Load raw data

```bash
python ingestion/load_csv.py

```

3. Run profiling
```bash
python profiling/profiling_engine.py
```

4. Construct baselines
```bash
python baseline/baseline_construction.py
```

5. Audit baselines
```bash
python baseline/baseline_audit.py
```

## Design Principles

Separation of ingestion, profiling, and baseline layers

Rolling baselines instead of static thresholds

Human validation before automated enforcement

Explicit documentation of known limitations

## Future Work

Threshold definition and alerting

Data freshness monitoring

Drift detection for numeric distributions

Integration with orchestration tools (e.g., Airflow)

## Author Notes

This project was designed as a learning-focused but production-realistic data quality system, emphasizing clarity of design over premature optimization.


## `requirements.txt`

```txt
pandas==2.2.3
psycopg2-binary==2.9.9
PyYAML==6.0.2
```