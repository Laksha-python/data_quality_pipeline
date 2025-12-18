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

def load_contract(path=None):
    if path is None:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(base_dir, "contracts", "data_contract.yaml")

    with open(path, "r") as f:
        return yaml.safe_load(f)


def enforce_rolling_window(cursor, table_name, column_name, metric_name, window_end):
    cutoff_date = window_end - timedelta(days=WINDOW_DAYS - 1)
    cursor.execute(
        """
        DELETE FROM dq.dq_baseline_stats
        WHERE table_name = %s
          AND column_name = %s
          AND metric_name = %s
          AND window_end::date < %s
        """,
        (table_name, column_name, metric_name, cutoff_date)
    )


def insert_baseline(cursor, table_name, column_name, metric_name, metric_value, window_start, window_end):
    if metric_value is None:
        return
    if hasattr(metric_value, "item"):
        metric_value = metric_value.item()
    metric_value = str(metric_value)
    
    cursor.execute(
        """
        INSERT INTO dq.dq_baseline_stats
        (table_name, column_name, metric_name, metric_value, window_start, window_end)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (table_name, column_name, metric_name, metric_value, window_start, window_end)
    )



def profile_table(cursor, conn, table_name, columns, day):
    full_table_name = f"{SCHEMA_RAW}.{table_name}" if "." not in table_name else table_name
    df = pd.read_sql(f"SELECT * FROM {full_table_name}", conn)
    if df.empty:
        print(f"WARNING: {full_table_name} is empty on {day.date()}")
        return

    window_end = day.date()
    window_start = (day - timedelta(days=WINDOW_DAYS - 1)).date()

    for col_name, col_type in columns.items():
        if col_name not in df.columns:
            continue

        series = df[col_name]
        non_null = series.dropna()

        metrics = {
            "record_count": len(series),
            "null_count": series.isna().sum(),
            "null_rate": series.isna().mean()
        }

        if col_type.lower() in ["int", "float"] and not non_null.empty:
            metrics.update({
                "min": non_null.min(),
                "max": non_null.max(),
                "mean": non_null.mean(),
                "std_dev": non_null.std()
            })

        if col_type.lower() in ["date", "datetime"] and not non_null.empty:
            metrics.update({
                "min": non_null.min(),
                "max": non_null.max()
            })


        for metric_name in metrics:
            enforce_rolling_window(cursor, full_table_name, col_name, metric_name, window_end)

        for metric_name, metric_value in metrics.items():
            insert_baseline(cursor, full_table_name, col_name, metric_name,
                            metric_value, window_start, window_end)


def main():
    contract = load_contract()
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    start_date = datetime.today() - timedelta(days=WINDOW_DAYS)

    for day_offset in range(1, WINDOW_DAYS + 1):
        day = start_date + timedelta(days=day_offset)
        print(f"\nProcessing Day {day_offset}/{WINDOW_DAYS}: {day.date()}")

        for table_name, table_contract in contract["tables"].items():
            columns = {col: str(col_props["type"]).lower()
                       for col, col_props in table_contract.get("columns", {}).items()}
            profile_table(cursor, conn, table_name, columns, day)

        conn.commit()

    cursor.close()
    conn.close()
    print("\n30-Day Rolling Baseline Construction Complete ✅")


if __name__ == "__main__":
    main()
