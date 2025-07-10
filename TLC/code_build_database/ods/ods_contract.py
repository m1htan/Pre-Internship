import pandas as pd
import pyodbc
from datetime import datetime
import calendar
from TLC.config_sql_server import config_sql_server
from TLC.config_sql_server.config_sql_server import config_sql_server_ods

# Constants & Mappings
SOURCE_TABLES = ['stg_barchart_HO_uco_price', 'stg_barchart_LF_uco_price']
ODS_TABLE_NAME = 'ods_contract'

MONTH_MAP = {'F': '1', 'G': '2', 'H': '3', 'J': '4', 'K': '5', 'M': '6',
             'N': '7', 'Q': '8', 'U': '9', 'V': '10', 'X': '11', 'Z': '12'}


def init_db():
    """Initialize database connections."""
    conn_stg = config_sql_server(section='sqlserver_stg')
    conn_ods = config_sql_server_ods(section='sqlserver_ods')
    return conn_stg, conn_ods


def fetch_existing_contracts(conn_ods):
    """Fetch contract_id and contract_code from ODS."""
    query = "SELECT contract_id, contract_code FROM ods_contract"
    df = pd.read_sql(query, conn_ods)
    return df


def extract_and_transform_contracts(conn_stg):
    """Extract and transform contract codes from all source tables."""
    df_all = pd.DataFrame(columns=['contract_code', 'original_contract'])

    for table in SOURCE_TABLES:
        sql = f"SELECT DISTINCT contract AS original_contract FROM {table}"
        df_temp = pd.read_sql(sql, conn_stg)

        if df_temp.empty:
            continue

        df_temp['contract_code'] = df_temp['original_contract'].str[-3:]
        df_all = pd.concat([df_all, df_temp[['contract_code', 'original_contract']]])

    # Drop duplicates
    df_all = df_all.drop_duplicates(subset='contract_code', keep='first')

    # Map contract month and year
    year_suffix = int(str(datetime.now().year)[-2:])
    year_threshold = year_suffix + 1

    df_all['mo_ship'] = df_all['original_contract'].str[2:3].map(MONTH_MAP)
    df_all['year_ship'] = df_all['original_contract'].str[-2:].apply(
        lambda x: '20' + x if int(x) <= year_threshold else '19' + x
    )

    return df_all.drop(columns='original_contract')


def insert_new_contracts(df_to_insert, conn_ods):
    """Insert new contracts into the ODS table."""
    if df_to_insert.empty:
        print("No new contracts to insert.")
        return

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    eff_dt = int(datetime.now().strftime("%Y%m%d"))

    df_to_insert['created_date'] = now_str
    df_to_insert['eff_dt'] = eff_dt
    df_to_insert['exp_dt'] = 20501231
    df_to_insert = df_to_insert[['contract_code', 'created_date', 'eff_dt', 'exp_dt', 'mo_ship', 'year_ship']]
    df_to_insert.columns = [col.lower() for col in df_to_insert.columns]

    try:
        with conn_ods.cursor() as cursor:
            cols = ', '.join(df_to_insert.columns)
            placeholders = ', '.join(['?' for _ in df_to_insert.columns])
            query = f"INSERT INTO {ODS_TABLE_NAME} ({cols}) VALUES ({placeholders})"

            for row in df_to_insert.itertuples(index=False, name=None):
                row = tuple(None if pd.isna(cell) else cell for cell in row)
                cursor.execute(query, row)

            conn_ods.commit()
            print(f"Inserted {len(df_to_insert)} new rows into {ODS_TABLE_NAME}")
    except Exception as error:
        print(f"Error during insert: {error}")
        conn_ods.rollback()


def main():
    start = datetime.now()
    conn_stg, conn_ods = init_db()
    df_existing = fetch_existing_contracts(conn_ods)
    df_all = extract_and_transform_contracts(conn_stg)
    df_new = pd.merge(df_all, df_existing, on='contract_code', how='left', indicator=True)
    df_new = df_new[df_new['_merge'] == 'left_only'].drop(columns=['_merge'])

    if not df_new.empty:
        print(f"Found {len(df_new)} new contract(s): {df_new['contract_code'].tolist()}")
    else:
        print("No new contracts found.")

    insert_new_contracts(df_new, conn_ods)

    conn_stg.close()
    conn_ods.close()

    print(f"Done in {datetime.now() - start}")


if __name__ == '__main__':
    main()
