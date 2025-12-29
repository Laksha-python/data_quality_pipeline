import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))
import psycopg2
import os
from datetime import date

RUN_DATE = date.today()
SOURCE_SCHEMA = "raw"

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME","data_quality_db"),
    "user": os.getenv("DB_USER","postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST","localhost"),
    "port": os.getenv("DB_PORT", "5432")
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def load_baseline_schema(conn):
    query = """
        SELECT table_name, column_name, data_type
        FROM dq.dq_schema_snapshot
        WHERE snapshot_date = (
            SELECT MIN(snapshot_date) FROM dq.dq_schema_snapshot
        );
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return {(r[0], r[1]): r[2] for r in cur.fetchall()}

def load_current_schema(conn):
    query = """
        SELECT
            table_schema || '.' || table_name,
            column_name,
            data_type
        FROM information_schema.columns
        WHERE table_schema = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (SOURCE_SCHEMA,))
        return {(r[0], r[1]): r[2] for r in cur.fetchall()}

def insert_anomaly(conn, table, column, severity):
    """
    Schema drift is categorical → store numeric fields as NULL
    """
    query = """
        INSERT INTO dq.dq_anomalies (
            run_date,
            table_name,
            column_name,
            dimension,
            observed_value,
            expected_value,
            deviation_percent,
            z_score,
            severity,
            status,
            created_at
        )
        VALUES (%s,%s,%s,'schema',NULL,NULL,NULL,NULL,%s,'OPEN',NOW())
        ON CONFLICT DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(query, (RUN_DATE, table, column, severity))

def detect_schema_drift():
    conn = get_conn()

    baseline = load_baseline_schema(conn)
    current = load_current_schema(conn)

    anomaly_count = 0
    for (table, column), base_type in baseline.items():
        if (table, column) not in current:
            insert_anomaly(conn, table, column, "HIGH")
            anomaly_count += 1
        else:
            curr_type = current[(table, column)]
            if curr_type != base_type:
                insert_anomaly(conn, table, column, "HIGH")
                anomaly_count += 1

    for (table, column) in current.keys():
        if (table, column) not in baseline:
            insert_anomaly(conn, table, column, "LOW")
            anomaly_count += 1

    conn.commit()
    conn.close()

    print(f" Schema drift detection complete | anomalies={anomaly_count}")

if __name__ == "__main__":
    detect_schema_drift()
