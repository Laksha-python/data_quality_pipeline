import os
import yaml
import pandas as pd
import psycopg2
from datetime import datetime, timedelta

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "dbname": os.getenv("DB_NAME", "data_quality_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT", 5432))
}

SCHEMA_RAW = "raw"
WINDOW_DAYS = 30

def load_contract():
    contract_path = os.getenv("DATA_CONTRACT")
    if not contract_path:
        raise RuntimeError("DATA_CONTRACT not set")

    with open(contract_path, encoding="utf-8") as f:
        contract = yaml.safe_load(f)

    if not contract or "tables" not in contract:
        raise RuntimeError("Invalid contract")

    return contract


def enforce_rolling_window(cur, table_name, column_name, metric_name, window_end):
    cutoff = window_end - timedelta(days=WINDOW_DAYS - 1)
    cur.execute("""
        DELETE FROM dq.dq_baseline_stats
        WHERE table_name = %s
          AND column_name = %s
          AND metric_name = %s
          AND window_end < %s
    """, (table_name, column_name, metric_name, cutoff))


def insert_baseline(cursor, table_name, column_name,
                    metric_name, metric_value,
                    window_start, window_end):

    if metric_value is None:
        return

    if hasattr(metric_value, "isoformat"):
        metric_value = metric_value.isoformat()
    else:
        metric_value = str(metric_value)

    cursor.execute("""
        INSERT INTO dq.dq_baseline_stats
        (table_name, column_name, metric_name, metric_value, window_start, window_end)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (
        table_name,
        column_name,
        metric_name,
        metric_value,
        window_start,
        window_end
    ))


def profile_table(cur, conn, table_name, columns, day):
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    if df.empty:
        print(f"WARNING: {table_name} empty")
        return

    window_end = day.date()
    window_start = (day - timedelta(days=WINDOW_DAYS - 1)).date()

    for col, col_type in columns.items():
        if col not in df.columns:
            continue

        series = df[col]
        non_null = series.dropna()

        metrics = {
            "record_count": len(series),
            "null_count": series.isna().sum(),
            "null_rate": series.isna().mean()
        }

        if col_type in {"int", "float"} and not non_null.empty:
            metrics.update({
                "min": non_null.min(),
                "max": non_null.max(),
                "mean": non_null.mean(),
                "std_dev": non_null.std()
            })

        if col_type == "timestamp" and not non_null.empty:
            metrics.update({
                "min": non_null.min(),
                "max": non_null.max()
            })

        for m in metrics:
            enforce_rolling_window(cur, table_name, col, m, window_end)

        for m, v in metrics.items():
            insert_baseline(cur, table_name, col, m, v, window_start, window_end)


def main():
    contract = load_contract()
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    start_date = datetime.today() - timedelta(days=WINDOW_DAYS)

    for i in range(1, WINDOW_DAYS + 1):
        day = start_date + timedelta(days=i)
        print(f"\nProcessing Day {i}/{WINDOW_DAYS}: {day.date()}")

        for table_name, table_contract in contract["tables"].items():
            cols = {
                c: str(v.get("type", "string")).lower()
                for c, v in table_contract["columns"].items()
            }

            full_table = table_name if "." in table_name else f"{SCHEMA_RAW}.{table_name}"
            profile_table(cur, conn, full_table, cols, day)

        conn.commit()

    cur.close()
    conn.close()
    print("\n30-Day Rolling Baseline Construction Complete")


if __name__ == "__main__":
    main()
