import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import os
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "dbname": os.getenv("DB_NAME", "data_quality_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT", 5432))
}


BASELINE_TABLE = "dq.dq_baseline_stats"
WINDOW_DAYS = 30

NUMERIC_METRICS = (
    "record_count",
    "null_count",
    "null_rate",
    "min",
    "max",
    "mean",
    "std_dev"
)


def load_baseline_data(conn):
    cutoff_date = datetime.today().date() - timedelta(days=WINDOW_DAYS - 1)

    query = f"""
        SELECT
            table_name,
            column_name,
            metric_name,
            metric_value,
            window_end
        FROM {BASELINE_TABLE}
        WHERE window_end >= %s
          AND metric_name IN {NUMERIC_METRICS}
    """

    df = pd.read_sql(query, conn, params=(cutoff_date,))

    # Convert only truly numeric values
    df["metric_value"] = pd.to_numeric(df["metric_value"], errors="coerce")
    df = df.dropna(subset=["metric_value"])

    return df



def run_audit(df):
    print("\n BASELINE SANITY AUDIT\n")

    null_rate_df = df[df["metric_name"] == "null_rate"]
    avg_null_rate = null_rate_df["metric_value"].mean()
    print(f"Average Null Rate: {avg_null_rate:.4f}")

    record_count_df = df[df["metric_name"] == "record_count"]
    avg_record_count = record_count_df["metric_value"].mean()
    print(f"Average Record Count: {avg_record_count:.2f}")

    mean_df = df[df["metric_name"] == "mean"]
    std_df = df[df["metric_name"] == "std_dev"]

    if mean_df.empty:
        print("\nNo numeric mean metrics found.")
    else:
        print("\nMean Value Range:")
        print(f"Min Mean: {mean_df['metric_value'].min():.4f}")
        print(f"Max Mean: {mean_df['metric_value'].max():.4f}")

        print("\nStandard Deviation Range:")
        print(f"Min Std: {std_df['metric_value'].min():.4f}")
        print(f"Max Std: {std_df['metric_value'].max():.4f}")

    summary = pd.DataFrame({
        "avg_null_rate": [avg_null_rate],
        "avg_record_count": [avg_record_count],
        "min_mean": [mean_df["metric_value"].min()],
        "max_mean": [mean_df["metric_value"].max()],
        "min_std_dev": [std_df["metric_value"].min()],
        "max_std_dev": [std_df["metric_value"].max()],
        "audit_run_date": [datetime.today().date()]
    })

    return summary


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    df = load_baseline_data(conn)

    if df.empty:
        print(" No baseline data found. Check baseline construction job.")
        return

    audit_summary = run_audit(df)
    audit_summary.to_csv("baseline_audit_summary.csv", index=False)

    print("\n baseline_audit_summary.csv generated")
    conn.close()


if __name__ == "__main__":
    main()
