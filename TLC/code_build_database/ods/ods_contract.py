import pandas as pd
import pyodbc
from datetime import datetime, timedelta
import calendar
import os
from TLC.config_sql_server import config_sql_server
from TLC.config_sql_server.config_sql_server import config_sql_server_ods

## Khởi tạo kết nối DB
def init_db():
    conn_stg = config_sql_server(section='sqlserver_stg')
    conn_ods = config_sql_server_ods(section='sqlserver_ods')
    return conn_stg, conn_ods

def BMonthEnd():
    today = datetime.now()
    last_day = calendar.monthrange(today.year, today.month)[1]
    return datetime(today.year, today.month, last_day)

def BMonthBegin():
    today = datetime.now()
    return datetime(today.year, today.month, 1)

def fetch_ods_contract(table_name, conn_ods):
    cursor_ods = conn_ods.cursor()
    sql_ods_contract = '''
    SELECT contract_id, contract_code FROM ods_contract
    '''
    cursor_ods.execute(sql_ods_contract)
    tuples = cursor_ods.fetchall()
    print("Columns in ods_contract query:", ['contract_id', 'contract_code'])
    print("Number of columns in tuples:", len(tuples[0]) if tuples else 0)
    print("Sample data (first 5 rows):", tuples[:5])
    print("Check row lengths:", [len(row) for row in tuples[:10]] if tuples else [])
    cursor_ods.close()

    tuples_list = [[item for item in row] for row in tuples] if tuples else []
    df_contract = pd.DataFrame(tuples_list, columns=['contract_id', 'contract_code']) if tuples else pd.DataFrame(columns=['contract_id', 'contract_code'])
    return df_contract

def process_insert_ods_contract(ods_table, df_to_insert, conn_ods):
    if df_to_insert.empty:
        print("No new contracts to insert.")
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df_to_insert = df_to_insert.rename(columns={'contract_code': 'contract_code'})
    df_to_insert['created_date'] = now
    df_to_insert['eff_dt'] = int(datetime.now().strftime("%Y%m%d"))
    df_to_insert['exp_dt'] = 20501231

    df_to_insert = df_to_insert[['contract_code', 'created_date', 'eff_dt', 'exp_dt', 'mo_ship', 'year_ship']]
    df_to_insert.columns = [col.lower() for col in df_to_insert.columns]

    cursor = conn_ods.cursor()
    try:
        cols = ', '.join(df_to_insert.columns)
        placeholders = ', '.join(['?' for _ in df_to_insert.columns])
        query = f"INSERT INTO {ods_table} ({cols}) VALUES ({placeholders})"

        for row in df_to_insert.itertuples(index=False, name=None):
            row = tuple(None if pd.isna(cell) else cell for cell in row)
            cursor.execute(query, row)

        conn_ods.commit()
        print(f"Inserted {len(df_to_insert)} new rows into {ods_table}")
    except Exception as error:
        print(f"Error during insert: {error}")
        conn_ods.rollback()
    finally:
        cursor.close()

def checking_new_ods(source_table, sql, conn_stg, conn_ods, df_existing_contract, ods_table):
    # Load raw contract list from STG
    df_stg = pd.read_sql(sql, conn_stg)
    if df_stg.empty:
        print(f"[{source_table}] No contracts found.")
        return

    df_stg.columns = ['contract_code']
    df_stg['original_contract'] = df_stg['contract_code']         # Giữ toàn bộ mã gốc
    df_stg['contract_code'] = df_stg['original_contract'].str[-3:]  # Lấy 3 ký tự cuối để làm mã chính

    # Drop duplicates theo contract_code (chỉ lấy duy nhất 1 hợp đồng mỗi mã)
    df_stg = df_stg.drop_duplicates(subset='contract_code', keep='first')

    # Mapping contract month từ ký tự thứ 3
    mo_map = {'F': '1', 'G': '2', 'H': '3', 'J': '4', 'K': '5', 'M': '6',
              'N': '7', 'Q': '8', 'U': '9', 'V': '10', 'X': '11', 'Z': '12'}
    current_year_suffix = int(str(datetime.now().year)[-2:])
    year_threshold = current_year_suffix + 1

    df_stg['mo_ship'] = df_stg['original_contract'].str[2:3].map(mo_map)
    df_stg['year_ship'] = df_stg['original_contract'].str[-2:].apply(
        lambda x: '20' + x if int(x) <= year_threshold else '19' + x
    )

    # Lọc hợp đồng mới chưa có trong ODS
    df_new = pd.merge(df_stg, df_existing_contract, on='contract_code', how='left', indicator=True)
    df_new = df_new[df_new['_merge'] == 'left_only'].drop(columns=['_merge', 'original_contract'])

    if df_new.empty:
        print(f"[{source_table}] All contracts already exist.")
    else:
        print(f"[{source_table}] Found {len(df_new)} new unique contract(s): {df_new['contract_code'].tolist()}")
        process_insert_ods_contract(ods_table, df_new, conn_ods)


if __name__ == '__main__':
    conn_stg, conn_ods = init_db()
    table_name = 'ods_contract'
    start = datetime.now()

    # Truncate bảng ods_contract trước khi import
    try:
        with conn_ods.cursor() as cur:
            cur.execute("TRUNCATE TABLE ods_contract")
            conn_ods.commit()
            print("Truncated table ods_contract")
    except Exception as error:
        print(f"Error truncating table ods_contract: {error}")
        conn_ods.rollback()

    df_contract = fetch_ods_contract(table_name, conn_ods)

    for source_table in ['stg_barchart_HOQ25_uco_price', 'stg_barchart_HOU25_uco_price',
        'stg_barchart_HOV25_uco_price', 'stg_barchart_HOX25_uco_price',
        'stg_barchart_HOZ25_uco_price', 'stg_barchart_HOF26_uco_price',
        'stg_barchart_HOG26_uco_price',
        'stg_barchart_HOH26_uco_price', 'stg_barchart_HOJ26_uco_price',
        'stg_barchart_HOK26_uco_price', 'stg_barchart_HON26_uco_price',
        'stg_barchart_HOM26_uco_price', 'stg_barchart_HOQ26_uco_price',
        'stg_barchart_HOU26_uco_price', 'stg_barchart_HOV26_uco_price',
        'stg_barchart_HOX26_uco_price', 'stg_barchart_HOZ26_uco_price']:
        sql = f'''
        SELECT DISTINCT contract AS contract_code FROM {source_table}
        '''
        checking_new_ods(source_table, sql, conn_stg, conn_ods, df_contract, table_name)

    end = datetime.now()
    duration = end - start

    try:
        with conn_ods.cursor() as cur:
            cur.execute("SELECT * FROM ods_contract WHERE mo_ship IS NOT NULL")
            get_data = cur.fetchall()
            print("Number of columns in get_data:", len(get_data[0]) if get_data else 0)
            print("Sample get_data (first 5 rows):", get_data[:5])
            get_data_list = [[item for item in row] for row in get_data] if get_data else []
            df = pd.DataFrame(get_data_list, columns=[
                'contract_id', 'contract_code', 'created_date',
                'eff_dt', 'exp_dt', 'mo_ship', 'year_ship'
            ]) if get_data else pd.DataFrame(columns=[
                'contract_id', 'contract_code', 'created_date',
                'eff_dt', 'exp_dt', 'mo_ship', 'year_ship'
            ])

            for i, m, y in zip(df['contract_code'], df['mo_ship'], df['year_ship']):
                year = int(y)
                month = int(m)
                last_date = calendar.monthrange(year, month)[1]
                first_month = f'1/{month}/{year}'
                end_month = f'{last_date}/{month}/{year}'
                print(f"{i}: {first_month} - {end_month}")
    except Exception as error:
        print(f"==> Error: {error}")
        conn_ods.rollback()

    conn_stg.close()
    conn_ods.close()