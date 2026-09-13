import sqlite3
import yaml

cfg = yaml.safe_load(open("config.yaml", encoding="utf-8"))
a = cfg["analytics"]
conn = sqlite3.connect(a["hybrid"]["processed_db"])
print("processed:", conn.execute("SELECT COUNT(*) FROM processed_messages").fetchone()[0])
print("state:", dict(conn.execute("SELECT key, value FROM hybrid_state").fetchall()))
conn.close()

from nlp.embeddings import ChromaStore
e = a.get("embeddings", {})
s = ChromaStore(e["store_path"], e["collection"])
print("vectors:", s.count())
if s.count():
    for h in s.query("people discussing an outage", k=3):
        print("  ", h["similarity"], (h["clean_text"] or "")[:60])

from nlp.llm_ollama import OllamaClient
print("ollama:", "UP" if OllamaClient(a.get("llm", {})).available()
      else "DOWN (titles fall back to keywords)")