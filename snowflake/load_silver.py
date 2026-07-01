import platform
platform.libc_ver = lambda *args, **kwargs: ("", "")

import os
from datetime import datetime, timezone
from deltalake import DeltaTable
import snowflake.connector
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()

SILVER_PATH = "data/silver/energy"

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_WAREHOUSE = "ENERGY_WH"
SNOWFLAKE_DATABASE = "ENERGY_PIPELINE"
SNOWFLAKE_SCHEMA = "SILVER"

def load_private_key():
    with open("snowflake_rsa_key.p8", "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None
        )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

def read_silver_as_df():
    dt = DeltaTable(SILVER_PATH)
    df = dt.to_pandas()
    print(f"Read {len(df)} records from Silver Delta Lake")
    return df

def get_connection():
    private_key = load_private_key()
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        private_key=private_key,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

def create_table_if_not_exists(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS energy_silver (
            series_id STRING,
            source STRING,
            period STRING,
            state_code STRING,
            state_name STRING,
            fuel_type_code STRING,
            fuel_type_name STRING,
            metric STRING,
            value FLOAT,
            unit STRING,
            ingested_at STRING,
            transformed_at STRING,
            llm_summary STRING
        )
    """)
    print("Table energy_silver ready")
    cursor.close()

def load_dataframe(conn, df):
    from snowflake.connector.pandas_tools import write_pandas
    df.columns = [c.upper() for c in df.columns]
    success, num_chunks, num_rows, _ = write_pandas(
        conn,
        df,
        table_name="ENERGY_SILVER",
        auto_create_table=False
    )
    print(f"Loaded {num_rows} rows in {num_chunks} chunks. Success: {success}")

def run():
    df = read_silver_as_df()

    conn = get_connection()
    print("Connected to Snowflake")

    create_table_if_not_exists(conn)
    load_dataframe(conn, df)

    conn.close()
    print("Done.")

if __name__ == "__main__":
    run()