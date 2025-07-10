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

def fetch_existing_d_contract(conn_dtm):
    query = "SELECT contract_id FROM d_contract"
    return pd.read_sql(query, conn_dtm)

def load_to_d_contract(df, conn_dtm):
    if df.empty:
        print("No new rows to insert into d_contract.")
        return

    cursor = conn_dtm.cursor()
    try:
        df.columns = [col.lower() for col in df.columns]
        cols = ','.join(df.columns)
        placeholders = ','.join(['?' for _ in df.columns])
        insert_sql = f"INSERT INTO d_contract ({cols}) VALUES ({placeholders})"

        for row in df.itertuples(index=False, name=None):
            cursor.execute(insert_sql, tuple(None if pd.isna(cell) else cell for cell in row))

        conn_dtm.commit()
        print(f"Inserted {len(df)} new rows into d_contract.")
    except Exception as e:
        conn_dtm.rollback()
        print(f"Error inserting into d_contract: {e}")
    finally:
        cursor.close()

def main():
    conn_ods, conn_dtm = init_db_connections()
    try:
        # Extract from ODS
        df_ods = fetch_ods_contract(conn_ods)
        print(f"Fetched {len(df_ods)} rows from ods_contract.")

        # Extract from DTM
        df_dtm = fetch_existing_d_contract(conn_dtm)
        existing_ids = set(df_dtm['contract_id'].tolist())

        # Filter: only new contracts
        df_new = df_ods[~df_ods['contract_id'].isin(existing_ids)]

        print(f"{len(df_new)} new contracts to insert.")

        # Load only new contracts
        load_to_d_contract(df_new, conn_dtm)

    finally:
        conn_ods.close()
        conn_dtm.close()
        print("ETL process completed and connections closed.")

if __name__ == "__main__":
    main()
