import sqlite3

conn = sqlite3.connect("data/vault.db")
conn.execute("DELETE FROM message_analytics")
conn.commit()
conn.close()
print("message_analytics cleared. Ready for backfill.")