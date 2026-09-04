import sqlite3

c = sqlite3.connect("data/vault.db")

print("messages per source:", c.execute(
    "SELECT source, COUNT(*) FROM messages GROUP BY source"
).fetchall())

print("latest:")
for r in c.execute(
    "SELECT datetime(posted_at_us/1000000, 'unixepoch'), source, author_username, substr(text,1,50) "
    "FROM messages ORDER BY id DESC LIMIT 5"
):
    print(" ", r)

print("ledger blocks:", c.execute("SELECT COUNT(*) FROM ledger_blocks").fetchone()[0])
print("analytics rows:", c.execute("SELECT COUNT(*) FROM message_analytics").fetchone()[0])
print("graph edges:", c.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0])
print("graph metrics computed:", c.execute("SELECT COUNT(*) FROM graph_metrics").fetchone()[0])