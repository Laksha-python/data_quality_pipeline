import pandas as pd
import psycopg2
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "dbname": os.getenv("DB_NAME", "data_quality_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT", 5432))
}


def clean_dataframe(df):
    
    return df.where(pd.notnull(df), None)


def insert_dataframe(conn, table_name, df):
    cursor = conn.cursor()

    df = clean_dataframe(df)

    cols = list(df.columns)
    col_names = ",".join(cols)
    placeholders = ",".join(["%s"] * len(cols))

    query = f"""
        INSERT INTO {table_name} ({col_names})
        VALUES ({placeholders})
    """

    for _, row in df.iterrows():
        cursor.execute(query, tuple(row))

    conn.commit()
    cursor.close()


if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)

    print("Loading raw.customers")
    df = pd.read_csv("data/raw/olist_customers_dataset.csv")
    insert_dataframe(conn, "raw.customers", df)

    print("Loading raw.orders")
    df = pd.read_csv("data/raw/olist_orders_dataset.csv")
    insert_dataframe(conn, "raw.orders", df)

    print("Loading raw.order_items")
    df = pd.read_csv("data/raw/olist_order_items_dataset.csv")
    insert_dataframe(conn, "raw.order_items", df)

    print("Loading raw.order_payments")
    df = pd.read_csv("data/raw/olist_order_payments_dataset.csv")
    insert_dataframe(conn, "raw.order_payments", df)

    conn.close()
    print("All data loaded successfully.")
