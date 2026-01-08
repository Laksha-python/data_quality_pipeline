import yaml
import psycopg2
import pandas as pd
import os
import sys
import math
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "raw"



DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "data_quality_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

def resolve_dataset_id(conn):
    dataset_name = os.getenv("DATASET_NAME")
    if not dataset_name:
        raise RuntimeError("DATASET_NAME not set")

    with conn.cursor() as cur:
        cur.execute(
            "SELECT dataset_id FROM dq.dq_datasets WHERE dataset_name = %s",
            (dataset_name,)
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Dataset not registered: {dataset_name}")
        return row[0]

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def normalize_value(value):

    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def load_table(conn, logical_table_name, table_spec):
    source = table_spec.get("source")
    if not source or source.get("type") != "csv":
        print(f"Skipping {logical_table_name} (no CSV source)")
        return

    csv_rel_path = source.get("path")
    if not csv_rel_path:
        raise ValueError(f"CSV path missing for table {logical_table_name}")

    csv_path = ROOT / csv_rel_path

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)

    columns = list(table_spec["columns"].keys())
    df = df[columns]

    placeholders = ",".join(["%s"] * len(columns))
    col_names = ",".join([f'"{c}"' for c in columns])

    insert_sql = f"""
        INSERT INTO {logical_table_name} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
    """

    cur = conn.cursor()
    inserted = 0

    for row in df.itertuples(index=False, name=None):
        clean_row = tuple(normalize_value(v) for v in row)
        cur.execute(insert_sql, clean_row)
        inserted += 1

    conn.commit()
    cur.close()

    print(f"Loaded {inserted} rows into {logical_table_name}")


def main(contract_path):

    with open(contract_path, "r",encoding="utf-8") as f:
        contract = yaml.safe_load(f)

    conn = get_connection()

    for logical_table, table_spec in contract["tables"].items():
        print(f"\nDEBUG {logical_table} keys:", table_spec.keys())
        load_table(conn, logical_table, table_spec)

    conn.close()
    print("Data loading complete.")


# -------------------------
# ENTRY POINT
# -------------------------
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python load_csv.py <contract_path>")
        sys.exit(1)
    

    main(sys.argv[1])
    

