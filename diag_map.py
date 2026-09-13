"""diag_map.py — why is the social map empty? 4-step diagnosis."""
import sqlite3
import time

DB = "data/vault.db"

c = sqlite3.connect(DB)

print("STEP 1 — evidence in the vault")
print("  messages by source:", c.execute(
    "SELECT source, COUNT(*) FROM messages GROUP BY source").fetchall())
print("  messages with replies:", c.execute(
    "SELECT COUNT(*) FROM messages WHERE reply_to_external_id IS NOT NULL").fetchone()[0])

print("\nSTEP 2 — graph builder output")
print("  graph_edges by kind:", c.execute(
    "SELECT kind, COUNT(*) FROM graph_edges GROUP BY kind").fetchall() or "NONE")
print("  pending_edges (parents not yet arrived):", c.execute(
    "SELECT COUNT(*) FROM pending_edges").fetchone()[0])

print("\nSTEP 3 — graph analysis pass")
last = c.execute("SELECT MAX(computed_at_us) FROM graph_metrics").fetchone()[0]
if last:
    age_min = (time.time() - last / 1_000_000) / 60
    print(f"  graph_metrics rows: {c.execute('SELECT COUNT(*) FROM graph_metrics').fetchone()[0]}"
          f" | last computed {age_min:.1f} min ago")
else:
    print("  graph_metrics rows: 0  <-- ANALYSIS NEVER RAN (wait for the 5-min timer,")
    print("  or force it with: python force_graph.py)")

print("\nSTEP 4 — what the dashboard query would see")
if last:
    n = c.execute(
        "SELECT COUNT(DISTINCT gn.author_id) FROM graph_metrics gm"
        " JOIN graph_nodes gn ON gn.source=gm.source AND gn.author_id=gm.author_id"
        " WHERE gm.computed_at_us=(SELECT MAX(computed_at_us) FROM graph_metrics)"
    ).fetchone()[0]
    print(f"  latest-analysis nodes: {n}")
    print("  sample nodes:", c.execute(
        "SELECT source, author_id, username, in_w, pagerank FROM graph_metrics gm"
        " JOIN graph_nodes gn ON gn.source=gm.source AND gn.author_id=gm.author_id"
        " WHERE gm.computed_at_us=? ORDER BY gm.pagerank DESC LIMIT 5",
        (last,)).fetchall())
else:
    print("  skipped (no analysis yet)")

print("\n--- interpretation ---")
print("  S1=0 messages        -> collection problem, not the map")
print("  S1 ok, S2 NONE       -> builder not processing: check collector log for 'graph pass failed'")
print("  S2 ok, S3 never      -> the 5-minute analysis timer: run force_graph.py")
print("  S3 ok, S4=0          -> join bug: paste this output back to me")
print("  S4 ok, UI empty      -> frontend: check F12 console + /api/graph directly")
c.close()