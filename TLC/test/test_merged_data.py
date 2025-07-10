import os
import traceback
import pandas as pd

from TLC.code_build_database.staging.HO.stg_barchart_HOQ25_uco_price import (read_csv_file,
                                                                             extract_contract_from_filename,
                                                                             normalize_dataframe,
                                                                             check_existing_data,
                                                                             contracts_match,
                                                                             insert_replication,
                                                                             move_processed_file)

def insert_into_staging_return_df(source_path, temp_name, table_name, conn_stg, table_admin_da_name, conn_dtm):
    expected_contract = table_name.split('_')[2]
    arr = [f for f in os.listdir(source_path)
           if f.lower().endswith('.csv')
           and expected_contract.lower() in f.lower()]

    print("[DEBUG] File hợp lệ:", arr)

    df_all = pd.DataFrame()

    for file_name in filter(lambda f: f.endswith(".csv") and "barchart_loadms" in f, arr):
        try:
            file_path = os.path.join(source_path, file_name)
            df = read_csv_file(file_path)
            contract_code = extract_contract_from_filename(file_name)
            if contract_code is None:
                raise ValueError(f"[ERROR] Không xác định được contract từ tên file: {file_name}")
            df['contract'] = contract_code
            df = normalize_dataframe(df)
            df_all = pd.concat([df_all, df], ignore_index=True)

            # Gọi replication nếu có dữ liệu tồn tại
            snapshot_date = pd.to_datetime(df['timing'].iloc[0]).date()
            df_existing = check_existing_data(conn_stg, table_name, snapshot_date)
            if not df_existing.empty and contracts_match(df_existing, df):
                insert_replication(file_path, table_admin_da_name, source_path, conn_dtm)

            move_processed_file(file_path, source_path)

        except Exception:
            print(traceback.format_exc())
            continue

    return df_all if not df_all.empty else None
