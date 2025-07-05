import os
import re
import numpy as np
import pyodbc
import pandas as pd
import shutil
from datetime import *
import traceback
from configparser import ConfigParser
import sys
import csv
import requests, zipfile
from io import StringIO
from pandas import DataFrame
import faulthandler
from dateutil.relativedelta import relativedelta
from TLC.config_sql_server import config_sql_server
faulthandler.enable()

DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

## Khởi tạo kết nối DB
def init_db():
    conn_stg = config_sql_server(section='sqlserver_stg')
    conn_dtm = config_sql_server(section='sqlserver_dtm')
    return conn_stg, conn_dtm

# Hàm lấy metadata
def get_meta_data(table_name: str, conn_stg):
    query = """
    SELECT * 
    FROM stg_meta_data 
    WHERE target_name = ?
    """
    df_meta = pd.read_sql(query, conn_stg, params=[table_name])

    if df_meta.empty:
        raise ValueError(f"Không tìm thấy metadata cho target_name = '{table_name}'")

    # Lấy giá trị hàng đầu tiên
    source_path = df_meta['source_path'].values[0]
    source_name = df_meta['source_name'].values[0]
    temp_name = df_meta['temp_name'].values[0]

    return source_name, source_path, temp_name


def extract_contract_from_filename(filename: str) -> str:
    mapping = {
        'HON25': 'HON25',
        'HOQ25': 'HOQ25',
        'HOU25': 'HOU25',
        'HOV25': 'HOV25',
        'HOX25': 'HOX25',
        'HOZ25': 'HOZ25',
        'LFN25': 'LFN25',
        'LFQ25': 'LFQ25',
        'LFU25': 'LFU25',
        'LFV25': 'LFV25',
        'LFX25': 'LFX25',
        'LFZ25': 'LFZ25',
    }
    for key, val in mapping.items():
        if key in filename:
            return val
    return None  # hoặc raise Exception nếu không khớp

def get_expected_contract(table_name: str) -> str:
    m = re.search(r'_([A-Z]{2}[A-Z]\d{2})_', table_name)
    if not m:
        raise ValueError(f"Không parse được contract từ {table_name}")
    return m.group(1)

## Hàm chính xử lý dữ liệu ✅
def insert_into_staging(source_path, temp_name, table_name, conn_stg, table_admin_da_name, conn_dtm):
    expected_contract = table_name.split('_')[2]
    source_row = 0
    target_row = 0
    arr = [f for f in os.listdir(source_path)
           if f.lower().endswith('.csv')
           and expected_contract.lower() in f.lower()]

    print("[DEBUG] File hợp lệ:", arr)

    for file_name in filter(lambda f: f.endswith(".csv") and "barchart_loadms" in f, arr):
        try:
            file_path = os.path.join(source_path, file_name)
            df = read_csv_file(file_path)
            df['contract'] = expected_contract  # khóa chặt

            print("Các cột hiện có trong df:", df.columns.tolist())

            # Gán contract theo tên file
            contract_code = extract_contract_from_filename(file_name)
            if contract_code is None:
                raise ValueError(f"[ERROR] Không xác định được contract từ tên file: {file_name}")

            df['contract'] = contract_code  # THÊM DÒNG NÀY

            snapshot_date = pd.to_datetime(df['tradeTime'].iloc[0]).date()

            df_existing = check_existing_data(conn_stg, table_name, snapshot_date)
            if not df_existing.empty and contracts_match(df_existing, df):
                insert_replication(file_path, table_admin_da_name, source_path, conn_dtm)

            df = normalize_dataframe(df)
            source_row += df.shape[0]

            truncate_temp_table(conn_stg, temp_name)
            insert_dataframe_to_temp(df, conn_stg, temp_name)
            delete_duplicates(conn_stg, table_name, temp_name)
            insert_new_records(conn_stg, table_name, temp_name)

            conn_stg.commit()

            move_processed_file(file_path, source_path)

        except Exception:
            print(traceback.format_exc())
            conn_stg.rollback()

    return source_row, target_row


def read_csv_file(file_path):
    df = pd.read_csv(file_path)
    if df.empty:
        raise ValueError(f"File {file_path} rỗng.")
    return df


def check_existing_data(conn, table_name, snapshot_date):
    query = f"SELECT * FROM {table_name} WHERE snapshot_date = ?"
    return pd.read_sql(query, conn, params=[snapshot_date])


def contracts_match(df_old, df_new):
    return df_old.reset_index(drop=True).equals(df_new.reset_index(drop=True))

COL_MAPPING = {
    'tradeTime':      'timing',
    'openPrice':      'prev_open',
    'highPrice':      'high',
    'lowPrice':       'low',
    'lastPrice':      'last',
    'priceChange':    'price_change',
    'percentChange':  'percent_change',
    'openInterest':   'oi',
}

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Đổi tên cột
    df = df.rename(columns=COL_MAPPING, errors='ignore')
    df.columns = [c.lower() for c in df.columns]

    # tradeTime -> datetime
    if 'timing' in df.columns:
        df['timing'] = pd.to_datetime(
            df['timing'],
            errors='coerce',
            format='%m/%d/%Y'
        )

    def clean_numeric(series: pd.Series):
        return (
            series.astype(str)
                  .str.replace('[%,+,−,]', '', regex=True)
                  .str.strip()
                  .replace({'': pd.NA})
                  .apply(pd.to_numeric, errors='coerce')
        )

    for col in ['price_change', 'percent_change',
                'prev_open', 'high', 'low', 'last', 'volume', 'oi']:
        if col in df.columns:
            df[col] = clean_numeric(df[col])

    return df

def insert_dataframe_to_temp(df, conn, temp_name):
    table = f"{temp_name}"
    cols = list(df.columns)

    col_list = ','.join(f'[{c}]' for c in cols)  # [timing],[prev_open],...
    placeholders = ','.join(['?'] * len(cols))  # ?,?,?,?,?,...
    insert_sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"

    tuples = [tuple(None if pd.isna(x) else x for x in row) for row in df.to_numpy()]

    cur = conn.cursor()
    cur.fast_executemany = True
    cur.executemany(insert_sql, tuples)
    conn.commit()


def truncate_temp_table(conn, temp_name):
    query = f"TRUNCATE TABLE {temp_name}"
    conn.cursor().execute(query)


def delete_duplicates(conn, table_name, temp_name):
    query = f"""
            DELETE A
            FROM [{table_name}]       AS A
            WHERE EXISTS (SELECT 1
                          FROM [{temp_name}] AS B
                          WHERE ISNULL(A.contract,'0') = ISNULL(B.contract,'0')
                            AND ISNULL(A.timing,'1900-01-01') = ISNULL(B.timing,'1900-01-01'));
    """
    conn.cursor().execute(query)


def insert_new_records(conn, table_name, temp_name):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    insert_cols = (
        "timing, contract, prev_open, high, low, last, price_change, "
        "percent_change, volume, oi, raw, source_table, created_date, "
        "snapshot_date, snapshot_date_ol"
    )

    query = f"""
        INSERT INTO {table_name} ({insert_cols})
        SELECT
            timing, contract, prev_open, high, low, last, price_change,
            percent_change, volume, oi, raw,
            '{temp_name}', '{now}',
            CAST(timing AS DATE),
            CASE WHEN DATEPART(WEEKDAY, CAST(timing AS DATE)) = 2
                 THEN DATEADD(DAY, 3, CAST(timing AS DATE))
                 ELSE DATEADD(DAY, 1, CAST(timing AS DATE))
            END
        FROM {temp_name} A
        WHERE NOT EXISTS (
            SELECT 1 FROM {table_name} B
            WHERE B.contract = A.contract AND B.timing = A.timing
        );
    """
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()


def move_processed_file(file_path, base_dir):
    file_name = os.path.basename(file_path)
    name, ext = os.path.splitext(file_name)
    process_dir = os.path.join(base_dir, "process")
    os.makedirs(process_dir, exist_ok=True)
    new_path = os.path.join(process_dir, f"{name}_{datetime.now().strftime('%Y%m%d')}{ext}")
    shutil.move(file_path, new_path)


## Xử lý replication sang bảng khác
def read_and_prepare_data(csv_path, conn_admin_da):
    df_temp = pd.read_csv(csv_path)[['Time', 'Open', 'High', 'Low', 'Last', 'Volume', 'Contract', 'Open Interest']]
    df_temp['day_off'] = False

    sql_day_off = "SELECT date_time FROM barchart_day_off WHERE type='100'"
    df_day_off = pd.read_sql(sql_day_off, conn_admin_da)

    if not df_day_off[df_day_off['date_time'].astype(str) == df_temp['Time'][0]].empty:
        df_temp['day_off'] = True

    return df_temp


def map_date_oi(df_temp, conn_dtm):
    date_end = datetime.strptime(df_temp['Time'][0], DATE_FORMAT)
    date_start = date_end - timedelta(days=14)

    sql_map = f"""
    SELECT a.date_id, a.date_map
    FROM market_map a
    WHERE a.date_id BETWEEN '{date_start}' AND '{date_end}'
    """
    df_map = pd.read_sql(sql_map, conn_dtm)

    df_temp = df_temp.merge(df_map, how='left', left_on='Time', right_on='date_id')
    df_temp = df_temp.drop(columns=['date_id'])
    df_temp = df_temp.rename(columns={'date_map': 'date_oi'})
    df_temp = df_temp.sort_values('date_oi')
    df_temp['date_oi'] = df_temp['date_oi'].fillna(method='ffill')

    return df_temp


def map_contract_info(df_temp, conn_dtm):
    sql_contract = "SELECT * FROM d_contract"
    df_contract = pd.read_sql(sql_contract, conn_dtm)

    df_temp = df_temp.merge(
        df_contract[['contract_code', 'fbd', 'Ibd', 'fbd_no', 'Ibd_no', 'ftd_no', 'ltd_no']],
        how='left', left_on='Contract', right_on='contract_code'
    ).drop(columns=['contract_code'])

    return df_temp


def write_to_temp_csv(df_temp, source_path, table_name):
    df_temp['date_time'] = df_temp['Time'] + " 00:00:00"
    df_temp['created_date'] = datetime.now().strftime(DATETIME_FORMAT)

    tmp_path = os.path.join(source_path, f"{table_name}.csv")
    df_temp.to_csv(tmp_path, index=False)
    return tmp_path


def insert_to_admin_table(df_temp, conn_admin_da, table_name):
    cursor = conn_admin_da.cursor()

    try:
        sql_delete = f"""
        DELETE FROM {table_name}
        WHERE CAST(date_actual AS DATE) = CAST('{df_temp['Time'][0]}' AS DATE)
        """
        cursor.execute(sql_delete)

        sql_insert = f"""
        INSERT INTO {table_name} 
        (date_actual, open, high, low, close, volume, oi, date_oi, contract_code, day_off,
         fbd, Ibd, fbd_no, Ibd_no, ftd_no, ltd_no, date_time, created_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for _, row in df_temp.iterrows():
            cursor.execute(sql_insert, (
                row['Time'], row['Open'], row['High'], row['Low'], row['Last'], row['Volume'],
                row['Open Interest'], row['date_oi'], row['Contract'], row['day_off'],
                row['fbd'], row['Ibd'], row['fbd_no'], row['Ibd_no'],
                row['ftd_no'], row['ltd_no'], row['date_time'], row['created_date']
            ))

        conn_admin_da.commit()
        print(f"[INFO] Inserted to {table_name}")
    except Exception as e:
        print("[ERROR]", e)
        conn_admin_da.rollback()
    finally:
        cursor.close()

def init_db_admin_da():
    return config_sql_server(section='sqlserver_admin_da')

def insert_replication(csv_path, table_admin_da_name, source_path, conn_dtm):
    conn_admin_da = init_db_admin_da()
    try:
        df_temp = read_and_prepare_data(csv_path, conn_admin_da)
        df_temp = map_date_oi(df_temp, conn_dtm)
        df_temp = map_contract_info(df_temp, conn_dtm)
        temp_csv_path = write_to_temp_csv(df_temp, source_path, table_admin_da_name)
        insert_to_admin_table(df_temp, conn_admin_da, table_admin_da_name)
        os.remove(temp_csv_path)
    finally:
        conn_admin_da.close()


## Ghi log quá trình xử lý
def checking_logs(conn_stg, script_name, source_name, table_name, source_row, target_row, duration, date_time):
    sql = """
        INSERT INTO stg_checking_logs (
            script, 
            source_name, 
            target_name, 
            source_row, 
            target_row, 
            duration, 
            created_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    values = (
        script_name,
        source_name,
        table_name,
        source_row,
        target_row,
        duration,
        date_time
    )

    try:
        with conn_stg.cursor() as cur:
            cur.execute(sql, values)
            conn_stg.commit()
        print(f"[LOG] Ghi log thành công cho {script_name}")
    except Exception as e:
        print(f"[ERROR] Ghi log thất bại: {e}")
        conn_stg.rollback()


# Main
if __name__ == '__main__':
    conn_stg, conn_dtm = init_db()
    table_name = 'stg_barchart_HOU25_uco_price'
    script_name = 'stg_barchart_HOU25_uco_price.py'
    table_admin_da_name = 'barchart_daily_idn'

    start = datetime.now()
    try:
        source_name, source_path, temp_name = get_meta_data(table_name, conn_stg)
        source_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), source_path)

        source_row, target_row = insert_into_staging(
            source_dir,
            temp_name,
            table_name,
            conn_stg,
            table_admin_da_name,
            conn_dtm
        )
    except Exception as e:
        print(f"Lỗi khi lấy metadata hoặc insert dữ liệu: {e}")
        source_row, target_row = 0, 0

    end = datetime.now()

    duration = end - start
    date_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")

    checking_logs(
        conn_stg,
        script_name,
        source_name,
        table_name,
        source_row,
        target_row,
        duration.total_seconds(),
        date_time
    )

    conn_stg.close()
    conn_dtm.close()

    conn = None