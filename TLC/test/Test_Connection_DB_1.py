from TLC.config_sql_server.config_sql_server import config_sql_server
conn_ods = config_sql_server(section='sqlserver_stg')
cursor = conn_ods.cursor()
cursor.execute("SELECT * FROM stg_barchart_HO_uco_price")

data = cursor.fetchall()
print("Sample data:", data[:5])  # In 5 dòng đầu tiên
print("Number of columns in data:", len(data[0]) if data else 0)

