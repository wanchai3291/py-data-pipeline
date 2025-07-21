from cassandra.cluster import Cluster
from cassandra.query import dict_factory

# ⚠️ ถ้าอยู่ใน Docker Compose ให้ใช้ service name เช่น 'cassandra_db'
cluster = Cluster(["cassandra_db"])  # หรือ ["cassandra_db"]
session = cluster.connect("user_activity")  # ใช้ keyspace ที่สร้างไว้

# ทำให้ผลลัพธ์เป็น dict
session.row_factory = dict_factory

# Query ล่าสุด 5 event
query = """
    SELECT id, name, action, created_at 
    FROM event_logs;
"""

# ⚠️ หมายเหตุ: Cassandra ต้องมี index หรือ filter ที่ถูกต้องสำหรับ WHERE
rows = session.execute(query)

# แสดงผล
print("Latest Event Logs:")
for row in rows:
    print(f"- {row['created_at']}: {row['name']} did {row['action']}")
