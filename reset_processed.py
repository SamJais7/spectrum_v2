import sqlite3

c = sqlite3.connect("data/processed.db")
for t in ("processed_messages", "topic_clusters", "llm_summaries", "hybrid_state"):
    c.execute(f"DELETE FROM {t}")
c.commit()
c.close()

import shutil
shutil.rmtree("data/vector_store", ignore_errors=True)   # semantic cache rebuilds
print("derived layer cleared — restart collector; five_step re-processes from the vault")