import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

import psycopg2
from datetime import date
import os
from anomaly.severity_rules import distribution_severity

RUN_DATE = date.today()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME","data_quality_db"),
    "user": os.getenv("DB_USER","postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST","localhost"),
    "port": os.getenv("DB_PORT", "5432")
}
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def load_numeric_stats(conn):
    query = """
        SELECT
            c.table_name,
            c.column_name,
            MAX(CASE WHEN c.metric_name = 'mean' THEN c.metric_value::float END) AS current_mean,
            MAX(CASE WHEN c.metric_name = 'std_dev' THEN c.metric_value::float END) AS current_std,
            MAX(CASE WHEN b.metric_name = 'mean' THEN b.metric_value::float END) AS baseline_mean,
            MAX(CASE WHEN b.metric_name = 'std_dev' THEN b.metric_value::float END) AS baseline_std
        FROM dq.dq_current_stats c
        LEFT JOIN dq.dq_baseline_stats b
            ON c.table_name = b.table_name
           AND c.column_name = b.column_name
           AND c.metric_name = b.metric_name
        WHERE c.run_date = %s
          AND c.column_name IS NOT NULL
        GROUP BY c.table_name, c.column_name
    """

    with conn.cursor() as cur:
        cur.execute(query, (RUN_DATE,))
        rows = cur.fetchall()

    stats = []
    for r in rows:
        stats.append({
            "table_name": r[0],
            "column_name": r[1],
            "current_mean": r[2],
            "current_std": r[3],
            "baseline_mean": r[4],
            "baseline_std": r[5]
        })
    return stats


def compute_z_score(current, baseline_mean, baseline_std):
    MIN_STD_DEV = 1e-3 

    if baseline_std is None or baseline_std < MIN_STD_DEV:
       return None

    return (current - baseline_mean) / baseline_std

def detect_distribution_drift(conn, stats):
    anomaly_count = 0

    insert_sql = """
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
        VALUES (%s,%s,%s,'distribution',%s,%s,%s,%s,%s,'OPEN',NOW())
        ON CONFLICT DO NOTHING;
    """

    with conn.cursor() as cur:
        for s in stats:
            current_mean = s["current_mean"]
            baseline_mean = s["baseline_mean"]
            baseline_std = s["baseline_std"]

            if current_mean is None or baseline_mean is None or baseline_std is None:
                continue

            z_score = compute_z_score(current_mean, baseline_mean, baseline_std)
            if z_score is None:
                continue

            severity = distribution_severity(z_score)

            if severity is None:
                continue

            deviation_pct = (
                ((current_mean - baseline_mean) / baseline_mean) * 100
                if baseline_mean != 0 else None
            )

            cur.execute(
                insert_sql,
                (
                    RUN_DATE,
                    s["table_name"],
                    s["column_name"],
                    current_mean,
                    baseline_mean,
                    deviation_pct,
                    z_score,
                    severity
                )
            )

            anomaly_count += 1
            print(
                f" Distribution drift: "
                f"{s['table_name']}.{s['column_name']} | "
                f"z={z_score:.2f} | severity={severity}"
            )

    conn.commit()
    print(f" Distribution drift completed | anomalies={anomaly_count}")


if __name__ == "__main__":
    conn = get_conn()
    stats = load_numeric_stats(conn)
    detect_distribution_drift(conn, stats)
    conn.close()
