"""The reading engine loop: pulls unanalyzed messages in batches, enriches each
one with language + emotions + the author's demographic snapshot (as of that
moment), and writes it back linked to the post. Also usable offline:

    python -m nlp.worker --backfill     # catch up historical data once
"""

import argparse
import asyncio
import json
import logging
import sys

from analytics_schema import analytics_connect, ensure_schema
from ledger import now_us
from nlp.engines import make_engine, detect_language, infer_demographics

log = logging.getLogger("collector.nlp")


def analyze_pending(db_path: str, engine, batch: int) -> int:
    conn = analytics_connect(db_path)
    try:
        rows = conn.execute(
            "SELECT m.id, m.source, m.text, m.author_id FROM messages m "
            "LEFT JOIN message_analytics a ON a.message_rowid=m.id "
            "WHERE a.message_rowid IS NULL AND m.text IS NOT NULL "
            "ORDER BY m.id LIMIT ?", (batch,)).fetchall()
        if not rows:
            return 0
        out = []
        for mid, source, text, author_id in rows:
            lang, lang_conf = detect_language(text)
            emo = engine.score(text)
            age = loc = lang_b = intr = None
            if author_id:
                d = conn.execute(
                    "SELECT age_bucket, location, language, interest FROM author_demographics "
                    "WHERE source=? AND author_id=?", (source, author_id)).fetchone()
                if d is None:                      # first sight: infer from what we have
                    d0 = infer_demographics(None, lang)
                    conn.execute(
                        "INSERT INTO author_demographics (source, author_id, profile_revision,"
                        " age_bucket, age_conf, location, location_conf, language, language_conf,"
                        " interest, interest_conf, evidence_json, inferred_at_us)"
                        " VALUES (?,?,0,?,?,?,?,?,?,?,?,?,?)",
                        (source, author_id, d0["age_bucket"], d0["age_conf"], d0["location"],
                         d0["location_conf"], d0["language"], d0["language_conf"],
                         d0["interest"], d0["interest_conf"],
                         json.dumps(d0["evidence"], ensure_ascii=False), now_us()))
                    age, loc, lang_b, intr = (d0["age_bucket"], d0["location"],
                                              d0["language"], d0["interest"])
                else:
                    age, loc, lang_b, intr = d
            out.append((mid, now_us(), lang, lang_conf,
                        json.dumps(emo["emotions"], ensure_ascii=False),
                        emo["dominant"], emo["confidence"], age, loc, lang_b, intr,
                        engine.name))
        conn.execute("BEGIN IMMEDIATE")
        conn.executemany(
            "INSERT OR REPLACE INTO message_analytics (message_rowid, analyzed_at_us, language,"
            " language_conf, emotions_json, dominant_emotion, dominant_emotion_conf,"
            " author_age_bucket, author_location, author_language, author_interest, engine)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", out)
        conn.execute("COMMIT")
        return len(out)
    finally:
        conn.close()


async def run_nlp_loop(cfg, shutdown):
    acfg = cfg.get("analytics", {})
    n = acfg.get("nlp", {})
    db = acfg.get("db_path", cfg["storage"]["db_path"])
    engine = make_engine(n)
    log.info("NLP loop up (engine=%s)", engine.name)
    while not shutdown.is_set():
        try:
            done = await asyncio.to_thread(analyze_pending, db, engine,
                                           int(n.get("batch", 500)))
            if done:
                log.info("nlp: analyzed %d messages", done)
            if not done:
                await asyncio.sleep(int(n.get("poll_seconds", 2)))
            else:
                await asyncio.sleep(0.2)
        except Exception:
            log.exception("nlp cycle failed")
            await asyncio.sleep(5)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import yaml
    with open("config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    acfg = cfg.get("analytics", {})
    db = acfg.get("db_path", cfg["storage"]["db_path"])
    ensure_schema(db)
    eng = make_engine(acfg.get("nlp", {}))
    if "--backfill" in sys.argv:
        total = 0
        while True:
            k = analyze_pending(db, eng, 500)
            total += k
            if not k:
                break
            print(f"  analyzed {total} messages…", end="\r")
        print(f"\nbackfill complete: {total} messages analyzed")
    else:
        asyncio.run(run_nlp_loop(cfg, asyncio.Event()))