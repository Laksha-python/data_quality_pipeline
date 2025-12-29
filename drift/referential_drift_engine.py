import sys
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()


ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))
import psycopg2
from datetime import date
import os

RUN_DATE = date.today()
SOURCE_SCHEMA = "raw"

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432")
}
def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def discover_foreign_keys(conn):
    query = """
        SELECT
            tc.table_schema || '.' || tc.table_name AS child_table,
            kcu.column_name AS child_column,
            ccu.table_schema || '.' || ccu.table_name AS parent_table,
            ccu.column_name AS parent_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (SOURCE_SCHEMA,))
        return cur.fetchall()

def compute_violation_rate(conn, fk):
    child_table, child_col, parent_table, parent_col = fk

    query = f"""
        SELECT
            COUNT(*) FILTER (WHERE p.{parent_col} IS NULL)::FLOAT
            / COUNT(*)
        FROM {child_table} c
        LEFT JOIN {parent_table} p
          ON c.{child_col} = p.{parent_col};
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()[0] or 0.0

def severity_from_rate(rate):
    if rate >= 0.02:
        return "HIGH"
    elif rate >= 0.01:
        return "MEDIUM"
    elif rate >= 0.005:
        return "LOW"
    return None

def insert_anomaly(conn, table, column, rate, severity):
    query = """
        INSERT INTO dq.dq_anomalies (
            run_date, table_name, column_name,
            dimension, observed_value, expected_value,
            severity, status, created_at
        )
        VALUES (%s,%s,%s,'referential',%s,0,%s,'OPEN',NOW())
        ON CONFLICT DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(
            query,
            (RUN_DATE, table, column, rate, severity)
        )

def detect_referential_drift():
    conn = get_conn()
    fks = discover_foreign_keys(conn)

    anomaly_count = 0

    for fk in fks:
        rate = compute_violation_rate(conn, fk)
        severity = severity_from_rate(rate)

        if severity:
            child_table, child_col, _, _ = fk
            insert_anomaly(conn, child_table, child_col, rate, severity)
            anomaly_count += 1

    conn.commit()
    conn.close()

    print(f"Referential drift completed | anomalies={anomaly_count}")

if __name__ == "__main__":
    detect_referential_drift()
