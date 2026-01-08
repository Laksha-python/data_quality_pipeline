import yaml
import psycopg2
import os
import sys
from dotenv import load_dotenv

load_dotenv()

TYPE_MAP = {
    "string": "TEXT",
    "int": "INTEGER",
    "float": "DOUBLE PRECISION",
    "timestamp": "TIMESTAMP",
    "datetime": "TIMESTAMP",
    "date": "DATE",
    "boolean": "BOOLEAN"
}


DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "data_quality_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

def generate_schema(contract_path: str):
    with open(contract_path, "r") as f:
        contract = yaml.safe_load(f)

    if "tables" not in contract:
        raise ValueError("Invalid contract: missing `tables` section")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    for logical_table, spec in contract["tables"].items():
        if "columns" not in spec:
            raise ValueError(f"Table {logical_table} missing columns definition")

        physical_table = logical_table if "." in logical_table else f"raw.{logical_table}"

        column_defs = []

        for column_name, meta in spec["columns"].items():
            col_type = TYPE_MAP.get(str(meta.get("type", "string")), "TEXT")
            nullable = "NOT NULL" if meta.get("required", False) else ""

            column_defs.append(
                f'"{column_name}" {col_type} {nullable}'
            )

        pk = spec.get("primary_key")
        if pk:
            if isinstance(pk, list):
                pk_cols = ",".join(f'"{c}"' for c in pk)
            else:
                pk_cols = f'"{pk}"'
            column_defs.append(f"PRIMARY KEY ({pk_cols})")

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {physical_table} (
            {", ".join(column_defs)}
        );
        """

        cur.execute(ddl)

    conn.commit()
    cur.close()
    conn.close()

    print("Schema generation complete.")

if __name__ == "__main__":
    if len(sys.argv) == 2:
        contract_path = sys.argv[1]
    else:
        contract_path = os.getenv("DATA_CONTRACT")

    if not contract_path:
        raise RuntimeError("Contract path not provided (arg or DATA_CONTRACT)")
    generate_schema(contract_path)
