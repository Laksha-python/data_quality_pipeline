# run_pipeline.py
import os
import sys
import subprocess
import argparse
from pathlib import Path
from datetime import datetime, timedelta, date
import uuid
import psycopg2
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).parent

PIPELINE_STEPS = [
    ROOT / "contract_validator.py",
    ROOT / "schema" / "schema_generator.py",
    ROOT / "ingestion" / "load_csv.py",
    ROOT / "drift" / "schema_drift_engine.py",
    ROOT / "profiling" / "profiling_engine.py",
    ROOT / "baseline" / "baseline_construction.py",
    ROOT / "baseline" / "baseline_audit.py",
    ROOT / "comparison" / "comparison_engine.py",
    ROOT / "drift" / "distribution_drift_engine.py",
    ROOT / "drift" / "referential_drift_engine.py",
    ROOT / "anomaly" / "anomaly_engine.py",
    ROOT / "aggregation" / "aggregation_engine.py",
    ROOT / "scoring" / "scoring_engine.py",
    ROOT / "root_cause" / "root_cause_engine.py",
    ROOT / "alerting" / "alert_engine.py",
]

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "data_quality_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def ensure_run_history_table():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dq.dq_run_history (
            run_id TEXT PRIMARY KEY,
            dataset_id INTEGER NOT NULL,
            dataset_name TEXT NOT NULL,
            run_date DATE NOT NULL,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            status TEXT,
            failed_step TEXT
        );
    """)
    conn.commit()
    cur.close()
    conn.close()


def ensure_datasets_table():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dq.dq_datasets (
            dataset_id SERIAL PRIMARY KEY,
            dataset_name TEXT UNIQUE NOT NULL,
            contract_path TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            is_active BOOLEAN DEFAULT TRUE
        );
    """)
    conn.commit()
    cur.close()
    conn.close()


def init_run_history(run_id, dataset_id, dataset_name, run_date):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dq.dq_run_history (
            run_id, dataset_id, dataset_name, run_date, start_time, status
        )
        VALUES (%s,%s,%s,%s,NOW(),'RUNNING')
    """, (run_id, dataset_id, dataset_name, run_date))
    conn.commit()
    cur.close()
    conn.close()


def finalize_run_history(run_id, status, failed_step=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE dq.dq_run_history
        SET end_time = NOW(),
            status = %s,
            failed_step = %s
        WHERE run_id = %s
    """, (status, failed_step, run_id))
    conn.commit()
    cur.close()
    conn.close()

def resolve_dataset_id(conn, dataset_name):
    print(f"Resolving dataset_id for dataset: {dataset_name}")
    
    cur = conn.cursor()
    
    cur.execute("""
        SELECT dataset_id FROM dq.dq_datasets 
        WHERE dataset_name = %s
    """, (dataset_name,))
    
    result = cur.fetchone()
    
    if result:
        dataset_id = result[0]
        print(f"Found existing dataset_id: {dataset_id}")
    else:
        cur.execute("""
            INSERT INTO dq.dq_datasets (dataset_name, contract_path)
            VALUES (%s, %s)
            RETURNING dataset_id
        """, (dataset_name, "contracts/" + dataset_name + ".yaml"))
        
        result = cur.fetchone()
        dataset_id = result[0] if result else None
        print(f"Created new dataset_id: {dataset_id}")
    
    conn.commit()
    cur.close()
    conn.close()
    
    if not dataset_id:
        raise RuntimeError(f"Failed to resolve dataset_id for {dataset_name}")
    
    return dataset_id


def resolve_run_dates(args):
    if args.run_date:
        return [datetime.strptime(args.run_date, "%Y-%m-%d").date()]

    if args.start_date and args.end_date:
        start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        return [
            start + timedelta(days=i)
            for i in range((end - start).days + 1)
        ]

    return [date.today()]

def run_step(script: Path, contract_path: str):
    print(f"\n▶ Running {script.name}")
    result = subprocess.run(
        [sys.executable, str(script), contract_path],
        env=os.environ.copy()
    )

    if result.returncode != 0:
        raise RuntimeError(script.name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("contract_path")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--run-date")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    args = parser.parse_args()
    ensure_run_history_table()
    ensure_datasets_table()
    
    run_dates = resolve_run_dates(args)

    for run_date in run_dates:
        run_id = str(uuid.uuid4())
        conn = get_conn()
        dataset_id = resolve_dataset_id(conn, args.dataset)
        os.environ["DATASET_ID"] = str(dataset_id)
        os.environ["DATASET_NAME"] = args.dataset
        os.environ["DATA_CONTRACT"] = args.contract_path
        os.environ["RUN_DATE"] = str(run_date)

        print(
            f"\nStarting Data Quality Pipeline | "
            f"dataset={args.dataset} (id={dataset_id}) | run_date={run_date} | run_id={run_id}"
        )

        init_run_history(run_id, dataset_id, args.dataset, run_date)

        try:
            for step in PIPELINE_STEPS:
                run_step(step, args.contract_path)

            finalize_run_history(run_id, "SUCCESS")
            print("Pipeline completed successfully")

        except Exception as e:
            finalize_run_history(run_id, "FAILED", str(e))
            print(f"FAILED at step: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
