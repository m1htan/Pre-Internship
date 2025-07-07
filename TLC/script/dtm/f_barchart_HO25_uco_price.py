import os
import sys
import csv
import xlrd
import pyodbc
from config import *
import requests, zipfile
from io import StringIO
import pandas as pa
import datetime
from datetime import *
from pandas import DataFrame
from datetime import datetime
import shutil
from pandas.tseries.offsets import BDay
from TLC.config_sql_server import config_sql_server
from TLC.config_sql_server.config_sql_server import config_sql_server_ods

DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

## Khởi tạo kết nối DB
def init_db():
    conn_dtm = config_sql_server(section='sqlserver_dtm')
    conn_ods = config_sql_server_ods(section='sqlserver_ods')
    return conn_dtm, conn_ods

def process_insert_stm_fact(fact, col_dtm, df_dtm, conn_dtm):
    try:
        sql_truncate = """TRUNCATE TABLE %s""" % (fact)
        df_dtm.columns = [col.lower() for col in df_dtm.columns.to_list()]
        tuples = [tuple(None if str(cell).lower() in ["", "nan"] else cell for cell in x) for rx in df_dtm.to_numpy()]
        cols = ','.join(list(df_dtm.columns))
        query = "INSERT INTO %s(%s) VALUES %%s" % (fact, cols)

        cur_dtm = conn_dtm.cursor()
        cur_dtm.execute(sql_truncate)

        extras.execute_values(cur_dtm, query, tuples)
        conn_dtm.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn_dtm.rollback()
        cur_dtm.close()
        return 1
    print("Insert DTM" + fact + "success")
    cur_dtm.close()


def process_dtm_fact(conn_ods, conn_dtm):
    f_fact = 'f_HO25_price'
    sql_ods = """
  select
  a.date_id,
  a.contract_id,
  a.prev_contract_id,
  a.last,
  a.prev_last,
  a.spread,
  a.ma_200,
  a.ma_50,
  a.mo,
  a.change,
  a.prev_open,
  a.high,
  a.low,
  a.prev,
  a.volume,
  a.oi,
  a.date_oi_id,
  lag(b.last,'0'::interger) OVER (PARTITION BY b.date_id ORDER BY b.mo) AS ld_last_ly
