import os
import pandas as pd
import pyodbc
import calendar
from datetime import datetime
from TLC.config_sql_server import config_sql_server
from TLC.config_sql_server.config_sql_server import config_sql_server_ods

def init_db():
    conn_stg = config_sql_server(section='sqlserver_stg')
    conn_ods = config_sql_server_ods(section='sqlserver_ods')
    return conn_stg, conn_ods

def truncate_ods_table(conn_ods, ods_table):
    cursor = conn_ods.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {ods_table}")
        conn_ods.commit()
        print(f"Truncated table {ods_table}")
    except Exception as error:
        print(f"Error truncating table {ods_table}: {error}")
        conn_ods.rollback()
    finally:
        cursor.close()

def insert_ods_table(conn_ods, df_final, ods_table):
    if df_final.empty:
        print(f"No rows to insert into {ods_table}")
        return

    df_final.columns = [col.lower() for col in df_final.columns]
    tuples = [tuple(None if pd.isna(cell) else cell for cell in row) for row in df_final.to_numpy()]
    cols = ','.join(df_final.columns)
    placeholders = ','.join(['?' for _ in df_final.columns])
    query = f"INSERT INTO {ods_table} ({cols}) VALUES ({placeholders})"

    cursor = conn_ods.cursor()
    try:
        cursor.fast_executemany = True
        cursor.executemany(query, tuples)
        conn_ods.commit()
        print(f"Inserted {len(df_final)} rows into {ods_table}")
    except Exception as error:
        print(f"Error inserting into {ods_table}: {error}")
        conn_ods.rollback()
    finally:
        cursor.close()

def process_ods_table(stg_table, ods_table, conn_stg, conn_ods):
    # Load reference tables as dictionaries
    df_date = pd.read_sql("SELECT date_actual, date_id FROM ods_date", conn_ods)
    date_map = dict(zip(df_date['date_actual'], df_date['date_id']))

    df_contract = pd.read_sql("SELECT contract_code, contract_id, mo_ship, year_ship FROM ods_contract", conn_ods)
    contract_map = dict(zip(df_contract['contract_code'], df_contract['contract_id']))

    lbd_map = {}
    for _, row in df_contract.iterrows():
        if pd.notna(row['mo_ship']) and pd.notna(row['year_ship']):
            try:
                last_day = calendar.monthrange(int(row['year_ship']), int(row['mo_ship']))[1]
                lbd_map[row['contract_code']] = datetime(int(row['year_ship']), int(row['mo_ship']), last_day)
            except:
                lbd_map[row['contract_code']] = pd.NaT
        else:
            lbd_map[row['contract_code']] = pd.NaT

    # Load STG data
    sql_stg = f"""
        SELECT
            RIGHT(contract, 3) AS contract_code,
            last,
            LAG(last) OVER (ORDER BY snapshot_date) AS prev_last,
            ROUND(CAST(last AS FLOAT) - CAST(LAG(last) OVER (ORDER BY snapshot_date) AS FLOAT), 2) AS spread,
            ROUND(AVG(CAST(last AS FLOAT)) OVER (ORDER BY snapshot_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW), 4) AS ma_200,
            ROUND(AVG(CAST(last AS FLOAT)) OVER (ORDER BY snapshot_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW), 4) AS ma_50,
            timing,
            MONTH(snapshot_date) AS mo,
            ROUND(price_change, 4) AS change,
            [open] AS open_price, high, low, volume, oi,
            snapshot_date AS date_actual,
            snapshot_date_oi AS date_actual_oi
        FROM {stg_table}
    """
    df = pd.read_sql(sql_stg, conn_stg)

    # Map IDs and derived columns
    df['contract_id'] = df['contract_code'].map(contract_map)
    df['date_id'] = df['date_actual'].map(date_map)
    df['lbd'] = df['contract_code'].map(lbd_map)
    df['contract_date'] = pd.to_datetime(df['lbd'], errors='coerce')
    df['contract_date_id'] = df['contract_date'].map(date_map)
    df['contract_date_fmt'] = df['contract_date'].dt.strftime('%y %b')

    selected_columns = [
        'contract_id', 'open_price', 'mo', 'last', 'prev_last', 'change', 'high', 'low',
        'volume', 'oi', 'spread', 'ma_200', 'ma_50', 'date_id', 'contract_date_id',
        'lbd', 'contract_date', 'contract_date_fmt'
    ]
    df_final = df[selected_columns].drop_duplicates()

    truncate_ods_table(conn_ods, ods_table)
    insert_ods_table(conn_ods, df_final, ods_table)

if __name__ == "__main__":
    conn_stg, conn_ods = init_db()
    try:
        stg_tables = ['stg_barchart_HO_uco_price', 'stg_barchart_LF_uco_price']
        for stg_table in stg_tables:
            ods_table = stg_table.replace('stg_', 'ods_')
            print(f"\n--- Processing {stg_table} → {ods_table} ---")
            process_ods_table(stg_table, ods_table, conn_stg, conn_ods)
    except Exception as e:
        print(f"ETL process failed: {e}")
    finally:
        conn_stg.close()
        conn_ods.close()
