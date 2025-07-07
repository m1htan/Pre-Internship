from TLC.config_sql_server.config_sql_server import config_sql_server_ods

conn_ods = config_sql_server_ods()
cursor = conn_ods.cursor()
cursor.execute("SELECT * FROM [ods_date]")

data = cursor.fetchall()
print("Sample data:", data[:5])  # In 5 dòng đầu tiên
print("Number of columns in data:", len(data[0]) if data else 0)

