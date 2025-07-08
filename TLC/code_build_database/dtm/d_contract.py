import pandas as pd
import pyodbc
from datetime import datetime
from TLC.config_sql_server import config_sql_server
from TLC.config_sql_server.config_sql_server import config_sql_server_ods

def init_db_connections():
    conn_ods = config_sql_server_ods(section='sqlserver_ods')
    conn_dtm = config_sql_server(section='sqlserver_dtm')
    return conn_ods, conn_dtm

def fetch_ods_contract(conn_ods):
    query = """
        SELECT contract_id, contract_code, mo_ship, year_ship, eff_dt, exp_dt, created_date
        FROM ods_contract
    """
    return pd.read_sql(query, conn_ods)

def truncate_d_contract(conn_dtm):
    try:
        with conn_dtm.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE d_contract")
            conn_dtm.commit()
            print("Truncated table d_contract in DTM.")
    except Exception as e:
        conn_dtm.rollback()
        print(f"Error truncating d_contract: {e}")

def load_to_d_contract(df, conn_dtm):
    cursor = conn_dtm.cursor()
    try:
        df.columns = [col.lower() for col in df.columns]
        cols = ','.join(df.columns)
        placeholders = ','.join(['?' for _ in df.columns])
        insert_sql = f"INSERT INTO d_contract ({cols}) VALUES ({placeholders})"
        for row in df.itertuples(index=False, name=None):
            cursor.execute(insert_sql, tuple(None if pd.isna(cell) else cell for cell in row))
        conn_dtm.commit()
        print(f"Inserted {len(df)} rows into d_contract.")
    except Exception as e:
        conn_dtm.rollback()
        print(f"Error inserting into d_contract: {e}")
    finally:
        cursor.close()

def main():
    conn_ods, conn_dtm = init_db_connections()
    try:
        # Extract
        df_contract = fetch_ods_contract(conn_ods)
        print(f"Fetched {len(df_contract)} rows from ods_contract.")

        # Optional: Transform (e.g., deduplicate)
        df_contract = df_contract.drop_duplicates(subset=['contract_id'])

        # Load
        truncate_d_contract(conn_dtm)
        load_to_d_contract(df_contract, conn_dtm)

    finally:
        conn_ods.close()
        conn_dtm.close()
        print("ETL process completed and connections closed.")

if __name__ == "__main__":
    main()