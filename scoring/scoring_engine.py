import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from config.config_loader import load_thresholds

import psycopg2
from datetime import date
import os

DATASET_ID = os.getenv("DATASET_ID")
if not DATASET_ID:
    raise RuntimeError("DATASET_ID not set")
DATASET_ID = int(DATASET_ID)

CONFIG = load_thresholds()
RUN_DATE = date.fromisoformat(
    os.getenv("RUN_DATE", str(date.today()))
)

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "data_quality_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}
DIMENSION_WEIGHTS = {
    k: v["weight"]
    for k, v in CONFIG["dimensions"].items()
}

SEVERITY_BASE_PENALTY = CONFIG["severity_penalty"]


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def score_status(score: int) -> str:
    if score >= 90:
        return "OK"
    elif score >= 70:
        return "WARNING"
    return "CRITICAL"


def compute_dq_score():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dq.dq_score_history (
            run_date DATE,
            dataset_id INT,
            dq_score INT,
            status TEXT,
            top_issue TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (run_date, dataset_id)
        );
    """)
    cur.execute("""
        SELECT
            dimension,
            dominant_severity,
            penalty_multiplier
        FROM dq.dq_aggregated_anomalies
        WHERE run_date = %s
          AND dataset_id = %s;
    """, (RUN_DATE, DATASET_ID))

    rows = cur.fetchall()

    total_penalty = 0
    max_dimension_penalty = 0
    top_issue = None
    for dimension, severity, multiplier in rows:
        base_penalty = SEVERITY_BASE_PENALTY.get(severity, 0)
        multiplier = multiplier if multiplier is not None else 1

        raw_penalty = base_penalty * multiplier
        dimension_cap = DIMENSION_WEIGHTS.get(dimension, 0)

        dimension_penalty = min(raw_penalty, dimension_cap)
        total_penalty += dimension_penalty

        if dimension_penalty > max_dimension_penalty:
            max_dimension_penalty = dimension_penalty
            top_issue = dimension

    dq_score = max(0, int(100 - total_penalty))
    status = score_status(dq_score)

    cur.execute("""
        INSERT INTO dq.dq_score_history (
            run_date,
            dataset_id,
            dq_score,
            status,
            top_issue
        )
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (run_date, dataset_id)
        DO UPDATE SET
            dq_score = EXCLUDED.dq_score,
            status = EXCLUDED.status,
            top_issue = EXCLUDED.top_issue,
            created_at = NOW();
    """, (
        RUN_DATE,
        DATASET_ID,
        dq_score,
        status,
        top_issue
    ))

    conn.commit()
    cur.close()
    conn.close()

    print(
        f"DQ Score computed | "
        f"dataset_id={DATASET_ID} | "
        f"score={dq_score} | "
        f"status={status} | "
        f"top_issue={top_issue}"
    )


if __name__ == "__main__":

    compute_dq_score()
