import os
import pandas as pd
import pyodbc
from datetime import datetime
import calendar
from TLC.config_sql_server import config_sql_server
from TLC.config_sql_server.config_sql_server import config_sql_server_ods

DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

## Khởi tạo kết nối DB
def init_db():
    conn_stg = config_sql_server(section='sqlserver_stg')
    conn_ods = config_sql_server_ods(section='sqlserver_ods')
    return conn_stg, conn_ods

def insert_ods_table(conn_ods, df_final, ods_table):
    cursor = conn_ods.cursor()
    try:
        # Truncate table before inserting
        cursor.execute(f"TRUNCATE TABLE {ods_table}")
        conn_ods.commit()
        print(f"Truncated table {ods_table}")

        # Prepare data for insert
        df_final.columns = [col.lower() for col in df_final.columns]
        tuples = [tuple(None if pd.isna(cell) else cell for cell in row) for row in df_final.to_numpy()]
        cols = ','.join(df_final.columns)
        placeholders = ','.join(['?' for _ in df_final.columns])
        query = f"INSERT INTO {ods_table} ({cols}) VALUES ({placeholders})"

        for row in tuples:
            cursor.execute(query, row)
        conn_ods.commit()
        print(f"Insert into {ods_table} done")
    except Exception as error:
        print(f"Error: {error}")
        conn_ods.rollback()
    finally:
        cursor.close()

def process_ods_table(stg_table, ods_table, conn_stg, conn_ods):
    # Load ODS reference tables
    df_date = pd.read_sql("SELECT date_id, date_actual FROM ods_date", conn_ods)
    df_date['date_actual_oi'] = df_date['date_actual']
    print("ods_date loaded:", df_date.shape)

    df_contract = pd.read_sql("SELECT contract_id, contract_code, mo_ship, year_ship FROM ods_contract", conn_ods)
    print("ods_contract loaded:", df_contract.shape)

    # Calculate LBD (last business date)
    df_contract['lbd'] = df_contract.apply(
        lambda row: pd.to_datetime(
            f"{row['year_ship']}-{row['mo_ship']}-{calendar.monthrange(int(row['year_ship']), int(row['mo_ship']))[1]}",
            format="%Y-%m-%d"
        ) if pd.notna(row['mo_ship']) and pd.notna(row['year_ship']) else pd.NaT,
        axis=1
    )
    df_contract['prev_contract_code'] = df_contract['contract_code']

    # Load STG data
    sql_stg = f"""
        SELECT
    RIGHT(contract, 3) AS contract_code,
    COALESCE(LAG(contract) OVER (ORDER BY snapshot_date), 'NaN') AS prev_contract_code,
    last,
    LAG(last) OVER (ORDER BY snapshot_date) AS prev_last,
    ROUND(
        CAST(last AS NUMERIC) -
        CAST(LAG(last) OVER (ORDER BY snapshot_date) AS NUMERIC), 2
    ) AS spread,
    AVG(CAST(last AS FLOAT)) OVER (
        ORDER BY snapshot_date
        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
    ) AS ma_200,
    AVG(CAST(last AS FLOAT)) OVER (
        ORDER BY snapshot_date
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) AS ma_50,
    timing,
    MONTH(snapshot_date) AS mo,
    price_change AS change,
    [open] AS prev_open,
    high,
    low,
    volume,
    oi,
    snapshot_date AS date_actual,
    snapshot_date_oi AS date_actual_oi
FROM {stg_table}
    """
    df_stg = pd.read_sql(sql_stg, conn_stg)
    print("stg_table loaded:", df_stg.shape)

    # Merge with reference tables
    df_final = df_stg.merge(df_date[['date_actual', 'date_id']], on='date_actual', how='left')
    df_final = df_final.merge(df_contract[['contract_code', 'contract_id', 'lbd']], on='contract_code', how='left')
    df_final = df_final.merge(
        df_contract[['contract_code', 'contract_id']].rename(columns={
            'contract_code': 'prev_contract_code',
            'contract_id': 'prev_contract_id'
        }),
        on='prev_contract_code', how='left'
    )
    df_final = df_final.merge(
        df_date[['date_actual_oi', 'date_id']].rename(columns={
            'date_actual_oi': 'contract_date',
            'date_id': 'contract_date_id'
        }),
        left_on='date_actual_oi', right_on='contract_date', how='left'
    )

    # Feature Engineering
    df_final['contract_date'] = pd.to_datetime(df_final['lbd'], errors='coerce')
    df_final['contract_date_fmt'] = df_final['contract_date'].dt.strftime('%y %b')

    # Select required columns
    selected_columns = [
        'contract_id', 'prev_contract_id', 'prev_open', 'mo', 'last',
        'prev_last', 'change', 'high', 'low', 'volume', 'oi', 'spread',
        'ma_200', 'ma_50', 'date_id', 'contract_date_id',
        'lbd', 'contract_date', 'contract_date_fmt'
    ]
    df_final = df_final[selected_columns].drop_duplicates()

    # Insert into ODS
    insert_ods_table(conn_ods, df_final, ods_table)

if __name__ == "__main__":
    conn_stg, conn_ods = init_db()

    stg_tables = [
        'stg_barchart_HON25_uco_price', 'stg_barchart_HOQ25_uco_price',
        'stg_barchart_HOU25_uco_price', 'stg_barchart_HOV25_uco_price',
        'stg_barchart_HOX25_uco_price', 'stg_barchart_HOZ25_uco_price',
        'stg_barchart_LFN25_uco_price', 'stg_barchart_LFQ25_uco_price',
        'stg_barchart_LFU25_uco_price', 'stg_barchart_LFV25_uco_price',
        'stg_barchart_LFX25_uco_price', 'stg_barchart_LFZ25_uco_price'
    ]

    for stg_table in stg_tables:
        # sinh tên bảng ODS tương ứng bằng cách thay 'stg_' bằng 'ods_'
        ods_table = stg_table.replace('stg_', 'ods_')
        print(f"\n--- Processing {stg_table} → {ods_table} ---")
        try:
            process_ods_table(stg_table, ods_table, conn_stg, conn_ods)
        except Exception as e:
            print(f"Error processing {stg_table}: {e}")

    conn_stg.close()
    conn_ods.close()
