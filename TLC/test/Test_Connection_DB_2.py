import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=192.168.98.139,1433;"      # IP Mac A
    "DATABASE=TLC;"
    "UID=sa;"
    "PWD=Minhtan0410@;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
    "Connection Timeout=5;"
)

print("Kết nối thành công:", conn.getinfo(pyodbc.SQL_SERVER_NAME))
