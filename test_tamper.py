import sqlite3

c = sqlite3.connect("data/vault.db")
try:
    c.execute("UPDATE messages SET text='FORGED' WHERE id=1")
    print("FAIL – tampering was allowed!")
except Exception as e:
    print("✓ tamper blocked by trigger:", str(e)[:60], "...")
c.close()