"""force_graph.py — run the 5-minute graph analysis immediately, then
re-run the edge builder first so nothing is stale. Use before demos."""
import yaml

from graph.analysis import compute_all
from graph.builder import build_pass

cfg = yaml.safe_load(open("config.yaml", encoding="utf-8"))
db = cfg["storage"]["db_path"]

n = 0
while True:                       # drain all unprocessed messages into edges
    k = build_pass(db)
    n += k
    if k == 0:
        break
print(f"edge builder: processed {n} messages")

compute_all(db, cfg["analytics"]["graph"])
print("analysis complete — refresh the dashboard (F5); nodes/edges should render now.")