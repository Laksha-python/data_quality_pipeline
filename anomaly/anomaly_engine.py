import psycopg2
import os
from datetime import date

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
    "port": os.getenv("DB_PORT", 5432)
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def infer_dimension(metric_name: str) -> str:
    metric = metric_name.lower()

    if "null" in metric:
        return "completeness"
    if "row_count" in metric or "volume" in metric:
        return "volume"
    if "mean" in metric or "std" in metric:
        return "distribution"
    if "fresh" in metric or "delay" in metric:
        return "freshness"
    if "schema" in metric:
        return "schema"
    if "fk" in metric or "referential" in metric:
        return "referential"

    return "other"


def compute_severity(deviation_percent):
    if deviation_percent is None:
        return "LOW"

    if deviation_percent >= 30:
        return "HIGH"
    elif deviation_percent >= 15:
        return "MEDIUM"
    else:
        return "LOW"


def insert_anomalies():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS dq;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dq.dq_anomalies (
            run_date DATE,
            dataset_id INT,
            dimension TEXT,
            table_name TEXT,
            column_name TEXT,
            metric_name TEXT,
            severity TEXT,
            deviation_percent FLOAT,
            baseline_value FLOAT,
            current_value FLOAT,
            persistence INT DEFAULT 1,
            created_at TIMESTAMP,
            PRIMARY KEY (run_date, dimension, table_name, column_name, metric_name)
        )
    """)

    cur.execute("""
        SELECT
            table_name,
            column_name,
            metric_name,
            percentage_deviation,
            baseline_mean,
            current_value
        FROM dq.dq_comparison_results
        WHERE run_date = %s
          AND status = 'ANOMALY'
    """, (RUN_DATE,))

    rows = cur.fetchall()
    inserted = 0

    for table_name, column_name, metric_name, deviation, baseline, current in rows:
        dimension = infer_dimension(metric_name)
        deviation_pct = abs(deviation) if deviation is not None else None
        severity = compute_severity(deviation_pct)

        cur.execute("""
            INSERT INTO dq.dq_anomalies (
                run_date,
                dataset_id,
                dimension,
                table_name,
                column_name,
                metric_name,
                severity,
                deviation_percent,
                baseline_value,
                current_value,
                persistence,
                created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1,NOW())
            ON CONFLICT (run_date, dimension, table_name, column_name, metric_name)
            DO UPDATE SET
                severity = EXCLUDED.severity,
                deviation_percent = EXCLUDED.deviation_percent,
                baseline_value = EXCLUDED.baseline_value,
                current_value = EXCLUDED.current_value,
                persistence = dq.dq_anomalies.persistence + 1,
                created_at = NOW()
        """, (
            RUN_DATE,
            DATASET_ID,
            dimension,
            table_name,
            column_name,
            metric_name,
            severity,
            deviation_pct,
            baseline,
            current
        ))

        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Anomaly engine completed | anomalies inserted: {inserted}")


if __name__ == "__main__":
    insert_anomalies()
