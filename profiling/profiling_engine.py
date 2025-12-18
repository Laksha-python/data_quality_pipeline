import yaml
import psycopg2
import pandas as pd
from datetime import date
from math import sqrt
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "dbname": os.getenv("DB_NAME", "data_quality_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT", 5432))
}


DATA_CONTRACT_PATH = "contracts/data_contract.yaml"


def insert_metric(cursor, table_name, column_name, metric_name, metric_value):
    if metric_value is None:
        return

    if hasattr(metric_value, "item"):
        metric_value = metric_value.item()

    cursor.execute(
        """
        INSERT INTO dq.dq_current_stats
        (run_date, table_name, column_name, metric_name, metric_value)
        VALUES (CURRENT_DATE, %s, %s, %s, %s)
        """,
        (table_name, column_name, metric_name, str(metric_value))
    )



def profile_table(table_name, table_contract, conn):
    schema, table = table_name.split(".")
    df = pd.read_sql(f"SELECT * FROM {schema}.{table}", conn)
    cursor = conn.cursor()
    record_count = len(df)


    insert_metric(cursor, table_name, None, "record_count", record_count)

    for col_name, col_props in table_contract["columns"].items():
        col_type = col_props["type"]
        series = df[col_name]

        null_count = series.isna().sum()
        null_rate = null_count / record_count if record_count > 0 else 0

        insert_metric(cursor, table_name, col_name, "null_count", null_count)
        insert_metric(cursor, table_name, col_name, "null_rate", null_rate)

        if col_type in ["int", "float"]:
            non_null = series.dropna()

            if len(non_null) > 0:
                mean = non_null.mean()
                std_dev = sqrt(((non_null - mean) ** 2).mean())

                insert_metric(cursor, table_name, col_name, "min", non_null.min())
                insert_metric(cursor, table_name, col_name, "max", non_null.max())
                insert_metric(cursor, table_name, col_name, "mean", mean)
                insert_metric(cursor, table_name, col_name, "std_dev", std_dev)

        elif col_type in ["date", "datetime"]:
            non_null = series.dropna()
            if len(non_null) > 0:
                insert_metric(cursor, table_name, col_name, "min", non_null.min())
                insert_metric(cursor, table_name, col_name, "max", non_null.max())

    conn.commit()
    cursor.close()


def main():
    with open(DATA_CONTRACT_PATH, "r") as f:
        contract = yaml.safe_load(f)

    conn = psycopg2.connect(**DB_CONFIG)

    for table_name, table_contract in contract["tables"].items():
        print(f"Profiling {table_name}...")
        profile_table(table_name, table_contract, conn)

    conn.close()
    print("Profiling complete.")


if __name__ == "__main__":
    main()
