import os
import sys
import yaml
import psycopg2
from datetime import date
from dotenv import load_dotenv

load_dotenv()

DATASET_ID = os.getenv("DATASET_ID")
if not DATASET_ID:
    raise RuntimeError("DATASET_ID not set")
DATASET_ID = int(DATASET_ID)

RUN_DATE = date.fromisoformat(
    os.getenv("RUN_DATE", str(date.today()))
)

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "data_quality_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

TABLE_LEVEL_COL = "__table__"  

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


def load_contract():
    contract_path = sys.argv[1] if len(sys.argv) > 1 else os.getenv("DATA_CONTRACT")

    if not contract_path:
        print("Usage: python profiling_engine.py <contract_path>")
        sys.exit(1)

    with open(contract_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def profile_table(conn, table_name, columns):
    cur = conn.cursor()
    DATASET_ID=resolve_dataset_id(conn)

    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    record_count = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO dq.dq_current_stats
        (
            run_date,
            dataset_id,
            table_name,
            column_name,
            metric_name,
            metric_value,
            created_at
        )
        VALUES (%s,%s,%s,%s,'record_count',%s,NOW())
        ON CONFLICT DO NOTHING
    """, (
        RUN_DATE,
        DATASET_ID,
        table_name,
        TABLE_LEVEL_COL,  
        record_count
    ))

    for column in columns:
        cur.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE {column} IS NULL"
        )
        null_count = cur.fetchone()[0]
        null_rate = (null_count / record_count) if record_count else 0.0

        cur.execute("""
            INSERT INTO dq.dq_current_stats
            (
                run_date,
                dataset_id,
                table_name,
                column_name,
                metric_name,
                metric_value,
                created_at
            )
            VALUES
                (%s,%s,%s,%s,'null_count',%s,NOW()),
                (%s,%s,%s,%s,'null_rate',%s,NOW())
            ON CONFLICT DO NOTHING
        """, (
            RUN_DATE, DATASET_ID, table_name, column, null_count,
            RUN_DATE, DATASET_ID, table_name, column, null_rate
        ))

    conn.commit()
    cur.close()


def main():
    contract = load_contract()
    conn = get_connection()

    for table_name, table_spec in contract["tables"].items():
        print(f"Profiling {table_name}...")
        columns = list(table_spec.get("columns", {}).keys())
        profile_table(conn, table_name, columns)

    conn.close()
    print("Profiling complete.")


if __name__ == "__main__":
    main()
