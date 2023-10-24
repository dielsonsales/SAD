import sqlite3

conn = sqlite3.connect('DataWarehouse.db')
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

for table in tables:
  print(table[0])


table_name = 't_fact'

cursor.execute(f"SELECT * FROM {table_name}")
rows = cursor.fetchall()

max_limit = 30
limit = 0
for row in rows:
  if limit == max_limit:
    break
  print(row)
  limit = limit + 1


cursor.close()
conn.close()