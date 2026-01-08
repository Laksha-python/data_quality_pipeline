import psycopg2
from datetime import date
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

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
    "port": os.getenv("DB_PORT", "5432"),
}

SEVERITY_RANK = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def aggregate_anomalies():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dq.dq_aggregated_anomalies (
        run_date DATE NOT NULL,
        dataset_id INT NOT NULL,
        dimension TEXT NOT NULL,
        table_name TEXT NOT NULL,
        dominant_severity TEXT,
        anomaly_count INT,
        affected_columns TEXT,
        penalty_multiplier INT,
        created_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (run_date, dataset_id, dimension, table_name)
    );


    """)

    cur.execute("""
        SELECT
            dimension,
            table_name,
            severity,
            column_name
        FROM dq.dq_anomalies
        WHERE run_date = %s
          AND dataset_id = %s;
    """, (RUN_DATE, DATASET_ID))

    rows = cur.fetchall()

    if not rows:
        print("No anomalies to aggregate")
        conn.close()
        return

    grouped = {}

    for dimension, table_name, severity, column in rows:
        key = (dimension, table_name)

        if key not in grouped:
            grouped[key] = {
                "count": 0,
                "max_severity": severity,
                "columns": set()
            }

        grouped[key]["count"] += 1
        grouped[key]["columns"].add(column)

        if SEVERITY_RANK[severity] > SEVERITY_RANK[grouped[key]["max_severity"]]:
            grouped[key]["max_severity"] = severity

    for (dimension, table_name), data in grouped.items():
        cur.execute("""
            INSERT INTO dq.dq_aggregated_anomalies (
                run_date,
                dataset_id,
                dimension,
                table_name,
                dominant_severity,
                anomaly_count,
                affected_columns,
                penalty_multiplier
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (run_date, dataset_id, dimension, table_name)
            DO UPDATE SET
                dominant_severity = EXCLUDED.dominant_severity,
                anomaly_count = EXCLUDED.anomaly_count,
                affected_columns = EXCLUDED.affected_columns,
                penalty_multiplier = EXCLUDED.penalty_multiplier,
                created_at = NOW();
        """, (
            RUN_DATE,
            DATASET_ID,
            dimension,
            table_name,
            data["max_severity"],
            data["count"],
            ", ".join(sorted(data["columns"])),
            data["count"]  
        ))

    conn.commit()
    cur.close()
    conn.close()

    print(f"Aggregated anomalies for dataset_id={DATASET_ID}")


if __name__ == "__main__":
    aggregate_anomalies()
