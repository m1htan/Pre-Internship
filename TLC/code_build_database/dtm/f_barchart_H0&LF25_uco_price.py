import os
import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
from TLC.config_sql_server import config_sql_server
from TLC.config_sql_server.config_sql_server import config_sql_server_ods

DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

FLOAT_COLUMNS = [
    'open_price', 'last', 'prev_last', 'change',
    'high', 'low', 'volume', 'oi',
    'spread', 'ma_200', 'ma_50'
]

def init_db():
    conn_dtm = config_sql_server(section='sqlserver_dtm')
    conn_ods = config_sql_server_ods(section='sqlserver_ods')
    return conn_dtm, conn_ods

def extract_from_ods_table(ods_table, conn_ods):
    query = f"""
        SELECT
            a.contract_id,
            a.open_price,
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
            a.contract_date_id,
            a.lbd,
            a.contract_date,
            a.contract_date_fmt
        FROM {ods_table} a
        LEFT JOIN ods_contract b ON a.contract_id = b.contract_id
        LEFT JOIN ods_date c ON a.date_id = c.date_id
    """
    try:
        return pd.read_sql(query, conn_ods)
    except Exception as e:
        print(f"Error extracting from {ods_table}: {e}")
        return pd.DataFrame()

def process_insert_dtm_fact(table_name, df, conn_dtm):
    if df.empty:
        print(f"No data to insert into {table_name}")
        return

    cursor = conn_dtm.cursor()
    try:
        print(f"Truncating table {table_name}...")
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        conn_dtm.commit()

        df.columns = [col.lower() for col in df.columns]
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = df.where(pd.notnull(df), None)

        for col in FLOAT_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').round(4)

        tuples = [tuple(None if pd.isna(cell) else cell for cell in row) for row in df.to_numpy()]
        cols = ','.join(df.columns)
        placeholders = ','.join(['?' for _ in df.columns])
        query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

        cursor.fast_executemany = True
        cursor.executemany(query, tuples)
        conn_dtm.commit()

        print(f"Inserted {len(df)} rows into {table_name}")
    except Exception as e:
        print(f"Error inserting into {table_name}: {e}")
        conn_dtm.rollback()
    finally:
        cursor.close()

def main():
    conn_dtm, conn_ods = init_db()
    try:
        ods_tables = ['ods_barchart_HO_uco_price', 'ods_barchart_LF_uco_price']
        dtm_mapping = {
            'HO': 'f_barchart_HO_uco_price',
            'LF': 'f_barchart_LF_uco_price'
        }

        for ods_table in ods_tables:
            print(f"\nReading from {ods_table}...")
            df = extract_from_ods_table(ods_table, conn_ods)
            if df.empty:
                print(f"No data extracted from {ods_table}")
                continue

            for keyword, fact_table in dtm_mapping.items():
                if f"_{keyword}" in ods_table:
                    process_insert_dtm_fact(fact_table, df, conn_dtm)
                    break

        print("\n ETL completed successfully.")
    except Exception as e:
        print(f"\n ETL failed: {e}")
    finally:
        conn_ods.close()
        conn_dtm.close()

if __name__ == '__main__':
    main()
