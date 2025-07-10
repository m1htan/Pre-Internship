import os
import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
from TLC.config_sql_server import config_sql_server
from TLC.config_sql_server.config_sql_server import config_sql_server_ods

DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

def init_db():
    conn_dtm = config_sql_server(section='sqlserver_dtm')
    conn_ods = config_sql_server_ods(section='sqlserver_ods')
    return conn_dtm, conn_ods

def process_insert_dtm_fact(fact, df_dtm, conn_dtm):
    try:
        cursor = conn_dtm.cursor()
        cursor.execute(f"TRUNCATE TABLE {fact}")
        conn_dtm.commit()
        print(f"Truncated table {fact}")

        df_dtm.columns = [col.lower() for col in df_dtm.columns]
        df_dtm.replace([np.inf, -np.inf], np.nan, inplace=True)
        df_dtm = df_dtm.where(pd.notnull(df_dtm), None)

        float_columns = ['open', 'last', 'prev_last', 'change', 'high', 'low',
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

def extract_from_ods_table(ods_table, conn_ods):
    sql_ods = f"""
    SELECT
        a.contract_id,
        a.open,
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
        df = pd.read_sql_query(sql_ods, conn_ods)
        if df.empty:
            print(f"[{ods_table}] → No data.")
        else:
            df['source_table'] = ods_table  # Ghi lại nguồn
        return df
    except Exception as e:
        print(f"Error reading from {ods_table}: {e}")
        return pd.DataFrame()

def main():
    conn_dtm, conn_ods = init_db()

    ods_tables = [
        'ods_barchart_HO_uco_price', 'ods_barchart_LF_uco_price'
    ]

    df_ho_all = pd.DataFrame()
    df_lf_all = pd.DataFrame()

    for ods_table in ods_tables:
        print(f"Reading from {ods_table}...")
        df = extract_from_ods_table(ods_table, conn_ods)
        if df.empty:
            continue
        if '_HO' in ods_table:
            df_ho_all = pd.concat([df_ho_all, df], ignore_index=True)
        elif '_LF' in ods_table:
            df_lf_all = pd.concat([df_lf_all, df], ignore_index=True)

    # Insert vào bảng gộp
    if not df_ho_all.empty:
        process_insert_dtm_fact('f_barchart_HO_uco_price', df_ho_all, conn_dtm)
    else:
        print("No HO data to insert.")

    if not df_lf_all.empty:
        process_insert_dtm_fact('f_barchart_LF_uco_price', df_lf_all, conn_dtm)
    else:
        print("No LF data to insert.")

    conn_ods.close()
    conn_dtm.close()
    print("\nETL completed: 2 fact tables created → f_barchart_HO_uco_price & f_barchart_LF_uco_price")

if __name__ == '__main__':
    main()
