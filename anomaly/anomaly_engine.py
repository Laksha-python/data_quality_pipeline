import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))
import os
import psycopg2
from datetime import date

from anomaly.severity_rules import (
    completeness_severity,
    volume_severity,
    distribution_severity,
    referential_severity
)
from anomaly.explanations import explain

RUN_DATE = date.today()

MIN_BASELINE_STD = 1e-3         
MIN_PERCENT_CHANGE = 0.1      

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432")
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def fetch_comparisons(conn):
    query = """
        SELECT
            table_name,
            column_name,
            metric_name,
            baseline_mean,
            current_value,
            percentage_deviation,
            z_score
        FROM dq.dq_comparison_results
        WHERE run_date = %s
          AND status = 'OK';
    """
    with conn.cursor() as cur:
        cur.execute(query, (RUN_DATE,))
        return cur.fetchall()


def infer_dimension(metric_name):
    if metric_name == "null_rate":
        return "completeness"
    if metric_name == "record_count":
        return "volume"
    if metric_name in ("mean", "std_dev"):
        return "distribution"
    return None


def compute_severity(dimension, deviation_pct, z_score):
    if dimension == "completeness":
        return completeness_severity(abs(deviation_pct))
    if dimension == "volume":
        return volume_severity(abs(deviation_pct))
    if dimension == "distribution":
        return distribution_severity(z_score)
    if dimension == "referential":
        return referential_severity(deviation_pct)
    return None


def insert_anomaly(
    conn,
    table_name,
    column_name,
    dimension,
    observed,
    expected,
    deviation_pct,
    z_score,
    severity,
    description
):
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
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',NOW())
        ON CONFLICT DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                RUN_DATE,
                table_name,
                column_name,
                dimension,
                observed,
                expected,
                deviation_pct,
                z_score,
                severity
            )
        )


def run_anomaly_engine():
    conn = get_connection()
    comparisons = fetch_comparisons(conn)

    anomaly_count = 0

    for (
        table_name,
        column_name,
        metric,
        baseline,
        current,
        deviation_pct,
        z_score
    ) in comparisons:

        dimension = infer_dimension(metric)
        if not dimension:
            continue

        if dimension == "completeness":
            if baseline == 0 and current == 0:
                continue

        if dimension == "distribution":
            if z_score is None:
                continue

            if abs(baseline or 0) < MIN_BASELINE_STD:
                continue

            if deviation_pct is not None and abs(deviation_pct) < MIN_PERCENT_CHANGE:
                continue

        severity = compute_severity(
            dimension,
            deviation_pct or 0,
            z_score or 0
        )

        if severity is None:
            continue

        description = explain(
            dimension,
            table_name,
            column_name,
            current,
            baseline
        )

        insert_anomaly(
            conn,
            table_name,
            column_name,
            dimension,
            current,
            baseline,
            deviation_pct,
            z_score,
            severity,
            description
        )

        anomaly_count += 1

    conn.commit()
    conn.close()

    print(f" Anomaly engine completed | anomalies inserted: {anomaly_count}")

if __name__ == "__main__":
    run_anomaly_engine()
