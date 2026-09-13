"""check_engines.py — engine attribution audit for the five-step pipeline.

Verifies that engine_used labels are honest: what ran, actually got recorded
as having run. Also shows cache growth and routing telemetry.
Run anytime: python check_engines.py
"""

import sqlite3

DB = "data/processed.db"


def main():
    try:
        c = sqlite3.connect(DB)
    except sqlite3.Error as e:
        raise SystemExit(f"cannot open {DB}: {e}")

    total = c.execute("SELECT COUNT(*) FROM processed_messages").fetchone()[0]
    if total == 0:
        print("processed.db is empty — run the collector first "
              "(five-step pipeline hasn't analyzed anything yet)")
        return

    print(f"processed messages: {total}")
    print("\n--- engine attribution (the audit field) ---")
    for engine, n in c.execute(
            "SELECT engine_used, COUNT(*) FROM processed_messages"
            " GROUP BY engine_used ORDER BY 2 DESC"):
        print(f"  {engine:20s} {n:6d}   ({100 * n / total:.1f}%)")

    print("\n--- provenance spot-check (rationale field) ---")
    for engine, n in c.execute(
            "SELECT engine_used, COUNT(*) FROM processed_messages"
            " WHERE rationale IS NOT NULL AND rationale<>''"
            " GROUP BY engine_used"):
        print(f"  {engine:20s} {n:6d} rows carry a rationale")

    print("\n--- live telemetry (hybrid_state) ---")
    for k, v in c.execute(
            "SELECT key, value FROM hybrid_state"
            " WHERE key IN ('cache_count','routing_reason','five_step_last')"
            " OR key LIKE 'engine_%' ORDER BY key"):
        print(f"  {k:20s} {v}")

    cache_ratio = c.execute(
        "SELECT COUNT(*) FROM processed_messages"
        " WHERE engine_used='cache_inherited'").fetchone()[0]
    print(f"\ncache hit ratio so far: {100 * cache_ratio / total:.1f}%")
    print("(early runs are layer1-heavy by design — the cache needs 500 vectors")
    print(" before inheritance activates; expect this to grow)")
    c.close()


if __name__ == "__main__":
    main()