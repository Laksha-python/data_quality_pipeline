import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

import psycopg2
import os
from datetime import date

from config.config_loader import load_thresholds

CONFIG = load_thresholds()

DATASET_ID = os.getenv("DATASET_ID")
if not DATASET_ID:
    raise RuntimeError("DATASET_ID not set")
DATASET_ID = int(DATASET_ID)

RUN_DATE = date.fromisoformat(
    os.getenv("RUN_DATE", str(date.today()))
)

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "data_quality_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
}

ALERT_CFG = CONFIG["alerts"]
WARNING_THRESHOLD = ALERT_CFG["warning_score"]
CRITICAL_THRESHOLD = ALERT_CFG["critical_score"]

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def fetch_latest_score(conn):
    query = """
        SELECT run_date, dq_score, status, top_issue
        FROM dq.dq_score_history
        WHERE dataset_id = %s
        ORDER BY run_date DESC
        LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(query, (DATASET_ID,))
        return cur.fetchone()

def fetch_high_severity_dimensions(conn, run_date):
    query = """
        SELECT dimension
        FROM dq.dq_aggregated_anomalies
        WHERE run_date = %s
          AND dataset_id = %s
          AND dominant_severity = 'HIGH';
    """
    with conn.cursor() as cur:
        cur.execute(query, (run_date, DATASET_ID))
        return [r[0] for r in cur.fetchall()]

def fetch_root_cause_summary(conn, run_date, dimension):
    query = """
        SELECT summary
        FROM dq.dq_root_causes
        WHERE run_date = %s
          AND dataset_id = %s
          AND dimension = %s
        LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(query, (run_date, DATASET_ID, dimension))
        row = cur.fetchone()
        return row[0] if row else None

def trigger_alert(run_date, score, level, top_issue, summary):
    print("\nALERT TRIGGERED")
    print(f"Run Date     : {run_date}")
    print(f"DQ Score     : {score} ({level})")
    print(f"Top Issue    : {top_issue}")
    if summary:
        print(f"Summary      : {summary}")
    else:
        print("Summary      : Root cause summary not available")

def run_alert_engine():
    conn = get_conn()
    score_row = fetch_latest_score(conn)  
    if not score_row:
        print("No score history found — skipping alerts")
        conn.close()
        return

    run_date, dq_score, status, top_issue = score_row
    alert_level = None
    if dq_score < CRITICAL_THRESHOLD:
        alert_level = "CRITICAL"
    elif dq_score < WARNING_THRESHOLD:
        alert_level = "WARNING"
    high_severity_dims = fetch_high_severity_dimensions(conn, run_date)

    if alert_level or high_severity_dims:
        if not alert_level:
            alert_level = "CRITICAL"
        dimension = top_issue or (high_severity_dims[0] if high_severity_dims else None)
        summary = None
        if dimension:
            summary = fetch_root_cause_summary(conn, run_date, dimension)
        trigger_alert(
            run_date=run_date,
            score=dq_score,
            level=alert_level,
            top_issue=dimension,
            summary=summary
        )
    else:
        print("No alerts triggered")
    conn.close()

if __name__ == "__main__":
    run_alert_engine()
