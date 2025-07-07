import pyodbc
import pandas as pd

from TLC.config_sql_server.config_sql_server import config_sql_server_ods, config_sql_server_dtm


def init_db_connections():
    """
    Initialize DB connections to ODS and DTM systems.
    """
    conn_ods = config_sql_server_ods(section='sqlserver_ods')
    conn_dtm = config_sql_server_dtm(section='sqlserver_dtm')
    return conn_ods, conn_dtm


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all missing values (NaN, pd.NA, NaT) to None to be compatible with pyodbc.
    Round float columns, and specifically round ma_200 and ma_50 columns if present.
    """
    df = df.where(pd.notnull(df), None)

    # Round all float columns generally
    float_cols = [col for col in df.columns if pd.api.types.is_float_dtype(df[col])]
    for col in float_cols:
        df[col] = df[col].apply(lambda x: round(x, 2) if x is not None else None)

    # Specifically round ma_200 and ma_50 (in case not detected as float before)
    for ma_col in ['ma_200', 'ma_50']:
        if ma_col in df.columns:
            df[ma_col] = df[ma_col].apply(lambda x: round(x, 2) if x is not None else None)

    return df


def insert_to_fact_table(df: pd.DataFrame, conn_dtm, dest_table: str):
    """
    Insert cleaned DataFrame into SQL Server using pyodbc and executemany.
    """
    df = clean_dataframe(df)
    columns = list(df.columns)
    placeholders = ", ".join(["?"] * len(columns))
    insert_stmt = f"INSERT INTO {dest_table} ({', '.join(columns)}) VALUES ({placeholders})"

    cursor = conn_dtm.cursor()
    try:
        cursor.fast_executemany = True
        cursor.executemany(insert_stmt, df.values.tolist())
        conn_dtm.commit()
        print(f"Inserted into {dest_table} successfully.")
    except Exception as e:
        print(f"Error inserting into {dest_table}: {e}")
    finally:
        cursor.close()


def process_table(src_table: str, dest_table: str, conn_ods, conn_dtm):
    """
    Extract data from src_table in ODS, clean it, and load into dest_table in DTM.
    """
    try:
        print(f"Processing {src_table} → {dest_table}...")
        df = pd.read_sql(f"SELECT * FROM {src_table}", conn_ods)
        if df.empty:
            print(f"Source table {src_table} is empty. Skipping.")
            return
        insert_to_fact_table(df, conn_dtm, dest_table)
    except Exception as e:
        print(f"Error processing {src_table}: {e}")


def main():
    # Initialize connections
    conn_ods, conn_dtm = init_db_connections()

    # List of contracts to process
    contracts = [
        "HON25", "HOQ25", "HOU25", "HOV25", "HOX25", "HOZ25",
        "LFN25", "LFQ25", "LFU25", "LFV25", "LFX25", "LFZ25"
    ]

    for contract in contracts:
        src_table = f"[ods_barchart_{contract}_uco_price]"
        dest_table = f"[f_barchart_{contract}_uco_price]"
        process_table(src_table, dest_table, conn_ods, conn_dtm)

    conn_ods.close()
    conn_dtm.close()
    print("ETL process completed.")


if __name__ == "__main__":
    main()
