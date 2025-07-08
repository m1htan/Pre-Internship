import os
import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
from TLC.config_sql_server import config_sql_server
from TLC.config_sql_server.config_sql_server import config_sql_server_ods

DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# Khởi tạo kết nối DB SQL Server
def init_db():
    conn_dtm = config_sql_server(section='sqlserver_dtm')
    conn_ods = config_sql_server_ods(section='sqlserver_ods')
    return conn_dtm, conn_ods

# Hàm insert dữ liệu vào bảng FACT
def process_insert_dtm_fact(fact, df_dtm, conn_dtm):
    try:
        cursor = conn_dtm.cursor()
        cursor.execute(f"TRUNCATE TABLE {fact}")
        conn_dtm.commit()
        print(f"Truncated table {fact}")

        df_dtm.columns = [col.lower() for col in df_dtm.columns]
        df_dtm.replace([np.inf, -np.inf], np.nan, inplace=True)
        df_dtm = df_dtm.where(pd.notnull(df_dtm), None)

        float_columns = ['prev_open', 'last', 'prev_last', 'change', 'high', 'low',
                         'volume', 'oi', 'spread', 'ma_200', 'ma_50']

        for col in float_columns:
            if col in df_dtm.columns:
                df_dtm[col] = pd.to_numeric(df_dtm[col], errors='coerce').round(4)

        tuples = [tuple(None if pd.isna(cell) else cell for cell in row) for row in df_dtm.to_numpy()]
        cols = ','.join(df_dtm.columns)
        placeholders = ','.join(['?' for _ in df_dtm.columns])
        query = f"INSERT INTO {fact} ({cols}) VALUES ({placeholders})"

        cursor.fast_executemany = True
        cursor.executemany(query, tuples)
        conn_dtm.commit()
        print(f"Insert into {fact} success")
    except Exception as e:
        print(f"Error inserting into {fact}: {e}")
        conn_dtm.rollback()
    finally:
        cursor.close()

# Hàm xử lý ETL cho từng bảng
def process_dtm_fact(ods_table, fact_table, conn_ods, conn_dtm):
    sql_ods = f"""
    SELECT
        a.contract_id,
        a.prev_contract_id,
        a.prev_open,
        a.mo,
        a.last,
        LAG(a.last) OVER (ORDER BY c.date_actual) AS prev_last,
        ROUND(a.change, 4) AS change,
        a.high,
        a.low,
        a.volume,
        a.oi,
        a.spread,
        a.ma_200,
        a.ma_50,
        a.date_id,
        a.date_id AS contract_date_id,
        a.lbd,
        a.contract_date,
        a.contract_date_fmt
    FROM {ods_table} a
    LEFT JOIN ods_contract b ON a.contract_id = b.contract_id
    LEFT JOIN ods_date c ON a.date_id = c.date_id
    """
    try:
        df_dtm = pd.read_sql_query(sql_ods, conn_ods)
    except Exception as e:
        print(f"Error reading from {ods_table}: {e}")
        return

    if df_dtm.empty:
        print(f"No data retrieved from {ods_table}.")
        return

    print(f"\nSample from {ods_table}:", df_dtm.head().to_dict(orient='records'))
    print("Data types:", df_dtm.dtypes.to_dict())
    process_insert_dtm_fact(fact_table, df_dtm, conn_dtm)

# Main function
def main():
    conn_dtm, conn_ods = init_db()

    ods_tables = [
        'ods_barchart_HON25_uco_price', 'ods_barchart_HOQ25_uco_price',
        'ods_barchart_HOU25_uco_price', 'ods_barchart_HOV25_uco_price',
        'ods_barchart_HOX25_uco_price', 'ods_barchart_HOZ25_uco_price',
        'ods_barchart_LFN25_uco_price', 'ods_barchart_LFQ25_uco_price',
        'ods_barchart_LFU25_uco_price', 'ods_barchart_LFV25_uco_price',
        'ods_barchart_LFX25_uco_price', 'ods_barchart_LFZ25_uco_price'
    ]

    for ods_table in ods_tables:
        fact_table = ods_table.replace('ods_', 'f_')
        print(f"\n--- Processing {ods_table} → {fact_table} ---")
        try:
            process_dtm_fact(ods_table, fact_table, conn_ods, conn_dtm)
        except Exception as e:
            print(f"Error processing {ods_table}: {e}")

    conn_ods.close()
    conn_dtm.close()
    print("\nETL for all tables completed.")

if __name__ == '__main__':
    main()
