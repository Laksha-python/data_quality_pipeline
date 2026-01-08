import psycopg2
import argparse
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "data_quality_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dq.dq_datasets (
                dataset_id SERIAL PRIMARY KEY,
                dataset_name TEXT UNIQUE NOT NULL,
                owner TEXT,
                contract_path TEXT,
                onboarded_at TIMESTAMP DEFAULT NOW()
            );
        """)
    conn.commit()


def register_dataset(dataset_name, contract_path):
    conn = get_conn()
    ensure_table(conn)

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dq.dq_datasets (dataset_name, contract_path)
            VALUES (%s, %s)
            ON CONFLICT (dataset_name)
            DO UPDATE SET contract_path = EXCLUDED.contract_path
            RETURNING dataset_id;
        """, (dataset_name, contract_path))

        dataset_id = cur.fetchone()[0]

    conn.commit()
    conn.close()

    print(f"✅ Dataset registered: {dataset_name} (dataset_id={dataset_id})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("contract_path")
    parser.add_argument("--dataset", required=True)
    args = parser.parse_args()

    register_dataset(args.dataset, args.contract_path)


if __name__ == "__main__":
    main()
