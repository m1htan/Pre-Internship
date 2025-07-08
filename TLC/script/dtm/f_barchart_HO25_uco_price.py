import os
import sys
import csv
import requests
import zipfile
from io import StringIO
import pandas as pd
from datetime import datetime
from pandas.tseries.offsets import BDay

from TLC.config_sql_server import config_sql_server
from TLC.config_sql_server.config_sql_server import config_sql_server_ods

DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# Khởi tạo kết nối DB SQL Server
def init_db():
    conn_dtm = config_sql_server(section='sqlserver_dtm')
    conn_ods = config_sql_server_ods(section='sqlserver_ods')
    return conn_dtm, conn_ods

# Hàm insert dữ liệu vào bảng fact
def process_insert_dtm_fact(fact, df_dtm, conn_dtm):
    try:
        cursor = conn_dtm.cursor()

        # Truncate bảng trước khi insert
        cursor.execute(f"TRUNCATE TABLE {fact}")

        df_dtm.columns = [col.lower() for col in df_dtm.columns]

        # Xử lý None với giá trị "nan"
        df_dtm = df_dtm.where(pd.notnull(df_dtm), None)

        # Tạo câu insert động
        columns = list(df_dtm.columns)
        col_str = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))
        insert_stmt = f"INSERT INTO {fact} ({col_str}) VALUES ({placeholders})"

        # Thực thi insert
        data = df_dtm.values.tolist()
        cursor.fast_executemany = True
        cursor.executemany(insert_stmt, data)
        conn_dtm.commit()
        print(f"Insert into {fact} success")
    except Exception as e:
        print(f"Error inserting into {fact}: {e}")
        conn_dtm.rollback()
    finally:
        cursor.close()

# Hàm truy vấn từ ODS và insert vào DTM
def process_dtm_fact(conn_ods, conn_dtm):
    f_fact = 'f_barchart_HON25_uco_price'

    sql_ods = """
    SELECT
        a.date_id,
        a.contract_id,
        a.prev_contract_id,
        a.last,
        a.prev_last,
        a.spread,
        ROUND(a.ma_200, 4) AS ma_200,
        ROUND(a.ma_50, 4) AS ma_50,
        a.mo,
        a.change,
        a.prev_open,
        a.high,
        a.low,
        a.volume,
        a.oi
    FROM ods_barchart_HON25_uco_price a
    LEFT JOIN ods_contract b 
        ON a.contract_id = b.contract_id
    LEFT JOIN ods_date c
        ON a.date_id = c.date_id
    """

    df_dtm = pd.read_sql(sql_ods, conn_ods)

    if df_dtm.empty:
        print("No data retrieved from ODS.")
        return

    process_insert_dtm_fact(f_fact, df_dtm, conn_dtm)

def main():
    conn_dtm, conn_ods = init_db()
    process_dtm_fact(conn_ods, conn_dtm)
    conn_ods.close()
    conn_dtm.close()
    print("ETL completed.")

if __name__ == '__main__':
    main()