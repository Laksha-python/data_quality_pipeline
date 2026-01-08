import psycopg2
import os
from datetime import date, timedelta

DATASET_ID = os.getenv("DATASET_ID")
if not DATASET_ID:
    raise RuntimeError("DATASET_ID not set")
DATASET_ID = int(DATASET_ID)

RUN_DATE = date.fromisoformat(os.getenv("RUN_DATE", str(date.today())))

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "data_quality_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5432),
}

SEVERITY_RANK = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def ensure_root_cause_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dq.dq_root_causes (
            run_date DATE,
            dataset_id INT,
            prev_score INT,
            current_score INT,
            dimension TEXT,
            table_name TEXT,
            columns TEXT,
            summary TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (run_date, dataset_id, dimension)
        );
        """)
    conn.commit()


def fetch_scores(conn, prev_date):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT run_date, dq_score
        FROM dq.dq_score_history
        WHERE dataset_id = %s
          AND run_date IN (%s, %s)
        """, (DATASET_ID, RUN_DATE, prev_date))
        return dict(cur.fetchall())


def fetch_aggregated_anomalies(conn):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT dimension, dominant_severity
        FROM dq.dq_aggregated_anomalies
        WHERE run_date = %s
          AND dataset_id = %s
        """, (RUN_DATE, DATASET_ID))
        return cur.fetchall()


def fetch_top_columns(conn, dimension):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT column_name
        FROM dq.dq_anomalies
        WHERE run_date = %s
          AND dataset_id = %s
          AND dimension = %s
        ORDER BY
          CASE severity
            WHEN 'HIGH' THEN 3
            WHEN 'MEDIUM' THEN 2
            ELSE 1
          END DESC
        LIMIT 3
        """, (RUN_DATE, DATASET_ID, dimension))
        return [r[0] for r in cur.fetchall()]


def insert_root_cause(conn, prev_score, curr_score, dimension, columns):
    summary = (
        f"DQ score dropped from {prev_score} to {curr_score} "
        f"due to {dimension} issues affecting columns: {', '.join(columns)}"
    )

    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO dq.dq_root_causes (
            run_date, dataset_id, prev_score, current_score,
            dimension, columns, summary
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
        """, (
            RUN_DATE, DATASET_ID, prev_score, curr_score,
            dimension, ", ".join(columns), summary
        ))
    conn.commit()


def run_root_cause_engine():
    conn = get_conn()
    ensure_root_cause_table(conn)
    PREV_DATE = RUN_DATE - timedelta(days=1)

    scores = fetch_scores(conn, PREV_DATE)
    if RUN_DATE not in scores or PREV_DATE not in scores:
        print("Not enough score history for root cause analysis")
        conn.close()
        return

    prev_score = scores[PREV_DATE]
    curr_score = scores[RUN_DATE]

    if curr_score >= prev_score:
        print("No score drop detected")
        conn.close()
        return

    aggregated = fetch_aggregated_anomalies(conn)
    if not aggregated:
        print("No aggregated anomalies found")
        conn.close()
        return

    aggregated.sort(
        key=lambda x: SEVERITY_RANK.get(x[1], 0),
        reverse=True
    )

    dimension, _ = aggregated[0]
    columns = fetch_top_columns(conn, dimension)

    insert_root_cause(conn, prev_score, curr_score, dimension, columns)

    conn.close()
    print("Root cause analysis completed")


if __name__ == "__main__":
    run_root_cause_engine()
