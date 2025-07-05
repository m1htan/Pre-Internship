from TLC.config_sql_server.config_sql_server import config_sql_server

conn = config_sql_server()
cursor = conn.cursor()
cursor.execute("SELECT * FROM Users")


rows = cursor.fetchall()

for row in rows:
    print(row)