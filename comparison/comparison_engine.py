from typing import Dict, Any, List, Tuple
import psycopg2
from datetime import datetime, date
import math
import csv
import os

def safe_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def compute_percentage_deviation(current: float, baseline: float):
    if baseline == 0:
        return None
    return (current - baseline) / baseline


def compute_absolute_deviation(current: float, baseline: float):
    return current - baseline


def compute_z_score(current: float, mean: float, std: float):
    if std == 0 or std is None:
        return 0.0
    return (current - mean) / std


def load_baseline_stats(conn) -> Dict[Tuple[str, str, str], Dict[str, float]]:
    query = """
        SELECT table_name, column_name, metric_name, metric_value
        FROM dq.dq_baseline_stats
    """
    stats = {}

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    grouped = {}
    for table, column, metric, value in rows:
        val = safe_float(value)
        if val is None:
            continue
        grouped.setdefault((table, column, metric), []).append(val)

    for key, values in grouped.items():
        mean = sum(values) / len(values)
        std = math.sqrt(sum((v - mean) ** 2 for v in values) / len(values))
        stats[key] = {"mean": mean, "std": std}

    return stats


def load_current_stats(conn) -> Dict[Tuple[str, str, str], float]:
    query = """
        SELECT table_name, column_name, metric_name, metric_value
        FROM dq.dq_current_stats
        WHERE run_date = CURRENT_DATE
    """
    stats = {}

    with conn.cursor() as cur:
        cur.execute(query)
        for table, column, metric, value in cur.fetchall():
            val = safe_float(value)
            if val is not None:
                stats[(table, column, metric)] = val

    return stats


def compare_stats(
    baseline_stats: Dict[Tuple[str, str, str], Dict[str, float]],
    current_stats: Dict[Tuple[str, str, str], float],
    z_cap: float = 10.0
) -> List[Dict[str, Any]]:

    results = []

    for key, current_value in current_stats.items():
        table, column, metric = key
        baseline = baseline_stats.get(key)

        if baseline is None:
            results.append({
                "table": table,
                "column": column,
                "metric": metric,
                "status": "NEW_METRIC",
                "baseline_mean": None,
                "current_value": current_value,
                "absolute_deviation": None,
                "percentage_deviation": None,
                "z_score": None
            })
            continue

        mean = baseline["mean"]
        std = baseline["std"]

        z = compute_z_score(current_value, mean, std)
        z = max(min(z, z_cap), -z_cap)

        results.append({
            "table": table,
            "column": column,
            "metric": metric,
            "status": "OK",
            "baseline_mean": mean,
            "current_value": current_value,
            "absolute_deviation": compute_absolute_deviation(current_value, mean),
            "percentage_deviation": compute_percentage_deviation(current_value, mean),
            "z_score": z
        })

    return results


def write_comparison_results(conn, comparisons):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dq.dq_comparison_results (
                run_date DATE,
                table_name TEXT,
                column_name TEXT,
                metric_name TEXT,
                status TEXT,
                baseline_mean DOUBLE PRECISION,
                current_value DOUBLE PRECISION,
                absolute_deviation DOUBLE PRECISION,
                percentage_deviation DOUBLE PRECISION,
                z_score DOUBLE PRECISION,
                created_at TIMESTAMP
            )
        """)

        insert_sql = """
            INSERT INTO dq.dq_comparison_results
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        now = datetime.now()

        for r in comparisons:
            cur.execute(insert_sql, (
                date.today(),
                r["table"],
                r["column"],
                r["metric"],
                r["status"],
                r["baseline_mean"],
                r["current_value"],
                r["absolute_deviation"],
                r["percentage_deviation"],
                r["z_score"],
                now
            ))

    conn.commit()


def export_to_csv(comparisons, path="comparison_results.csv"):
    keys = [
        "table", "column", "metric", "status",
        "baseline_mean", "current_value",
        "absolute_deviation", "percentage_deviation", "z_score"
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in comparisons:
            writer.writerow({k: row.get(k) for k in keys})


if __name__ == "__main__":
    DB_CONFIG = {
        "dbname": os.getenv("DB_NAME", "data_quality_db"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432")
    }

    conn = psycopg2.connect(**DB_CONFIG)

    baseline_stats = load_baseline_stats(conn)
    current_stats = load_current_stats(conn)

    comparisons = compare_stats(baseline_stats, current_stats)

    write_comparison_results(conn, comparisons)
    export_to_csv(comparisons)

    print(f"{len(comparisons)} metrics processed")

    conn.close()
