# check_step5.py
import sqlite3
c = sqlite3.connect("data/processed.db")
print("clusters:")
for r in c.execute("SELECT title, message_count, velocity_score"
                   " FROM topic_clusters ORDER BY message_count DESC LIMIT 10"):
    print("  ", r)
row = c.execute("SELECT COUNT(*), MAX(reason) FROM llm_summaries").fetchone()
print("briefings:", row)