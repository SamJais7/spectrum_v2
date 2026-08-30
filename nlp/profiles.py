"""Periodically fetch PUBLIC profile data (bio, declared location) for known
authors, and re-infer demographics whenever a profile changes."""

import asyncio
import json
import logging

from analytics_schema import analytics_connect
from ledger import now_us
from nlp.engines import infer_demographics

log = logging.getLogger("collector.profiles")


def upsert_profile(conn, source, author_id, username, bio, location, verified, raw):
    """Returns True if the profile content changed (revision bumped)."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT username, bio, location, revision FROM author_profiles "
                           "WHERE source=? AND author_id=?", (source, author_id)).fetchone()
        t = now_us()
        if row is None:
            conn.execute("INSERT INTO author_profiles (source, author_id, username, bio,"
                         " location, verified, fetched_at_us, revision, raw_json)"
                         " VALUES (?,?,?,?,?,?,?,1,?)",
                         (source, author_id, username, bio, location,
                          1 if verified else 0, t, json.dumps(raw, ensure_ascii=False, default=str)))
            changed = True
        elif (row[0], row[1], row[2]) != (username, bio, location):
            conn.execute("UPDATE author_profiles SET username=?, bio=?, location=?, verified=?,"
                         " fetched_at_us=?, revision=revision+1, raw_json=? "
                         "WHERE source=? AND author_id=?",
                         (username, bio, location, 1 if verified else 0, t,
                          json.dumps(raw, ensure_ascii=False, default=str), source, author_id))
            changed = True
        else:
            conn.execute("UPDATE author_profiles SET fetched_at_us=? WHERE source=? AND author_id=?",
                         (t, source, author_id))
            changed = False
        conn.execute("COMMIT")
        return changed
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def refresh_demographics(conn, source, author_id, language):
    """(Re)infer demographics after a profile change; append to history."""
    prof = conn.execute("SELECT bio, location, revision FROM author_profiles "
                        "WHERE source=? AND author_id=?", (source, author_id)).fetchone()
    if prof is None:
        return
    lang_row = conn.execute("SELECT language FROM author_demographics WHERE source=? AND author_id=?",
                            (source, author_id)).fetchone()
    lang = lang_row[0] if lang_row and lang_row[0] else (language or "en")
    d = infer_demographics({"bio": prof[0], "location": prof[1]}, lang)
    t = now_us()
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("""INSERT INTO author_demographics (source, author_id, profile_revision,
                     age_bucket, age_conf, location, location_conf, language, language_conf,
                     interest, interest_conf, evidence_json, inferred_at_us)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(source, author_id) DO UPDATE SET
                     profile_revision=excluded.profile_revision, age_bucket=excluded.age_bucket,
                     age_conf=excluded.age_conf, location=excluded.location,
                     location_conf=excluded.location_conf, language=excluded.language,
                     language_conf=excluded.language_conf, interest=excluded.interest,
                     interest_conf=excluded.interest_conf, evidence_json=excluded.evidence_json,
                     inferred_at_us=excluded.inferred_at_us""",
                 (source, author_id, prof[2], d["age_bucket"], d["age_conf"], d["location"],
                  d["location_conf"], d["language"], d["language_conf"], d["interest"],
                  d["interest_conf"], json.dumps(d["evidence"], ensure_ascii=False), t))
    conn.execute("INSERT INTO author_demographics_history (source, author_id, inferred_at_us,"
                 " age_bucket, location, language, interest) VALUES (?,?,?,?,?,?,?)",
                 (source, author_id, t, d["age_bucket"], d["location"], d["language"], d["interest"]))
    conn.execute("COMMIT")


def _candidates(conn, source, refresh_h_us, max_n):
    return conn.execute(
        "SELECT a.author_id, a.username FROM authors a "
        "LEFT JOIN author_profiles p ON p.source=a.source AND p.author_id=a.author_id "
        "WHERE a.source=? AND a.author_id IS NOT NULL "
        "AND (p.author_id IS NULL OR p.fetched_at_us < ?) "
        "ORDER BY a.last_seen_us DESC LIMIT ?", (source, now_us() - refresh_h_us, max_n)).fetchall()


async def run_x_profile_loop(bearer_token, cfg, shutdown):
    """Fetch X author profiles (bio/location) in batches of 100."""
    import tweepy
    acfg = cfg.get("analytics", {})
    p = acfg.get("profiles", {})
    refresh_us = int(p.get("refresh_hours", 12)) * 3_600_000_000
    client = tweepy.Client(bearer_token, wait_on_rate_limit=True)
    log.info("X profile refresher started")
    while not shutdown.is_set():
        try:
            conn = analytics_connect(acfg.get("db_path", cfg["storage"]["db_path"]))
            try:
                rows = _candidates(conn, "x", refresh_us, int(p.get("max_per_cycle", 1000)))
                for i in range(0, len(rows), 100):
                    if shutdown.is_set():
                        return
                    chunk = rows[i:i + 100]
                    resp = client.get_users(
                        ids=[r[0] for r in chunk],
                        user_fields=["id", "username", "description", "location",
                                     "verified", "public_metrics"])
                    for u in (resp.data or []):
                        changed = upsert_profile(conn, "x", str(u.id), u.username,
                                                 u.description, u.location, u.verified,
                                                 dict(u.public_metrics or {}))
                        if changed:
                            refresh_demographics(conn, "x", str(u.id), None)
                    await asyncio.sleep(2)
                if rows:
                    log.info("X profiles refreshed: %d authors", len(rows))
            finally:
                conn.close()
        except Exception:
            log.exception("X profile cycle failed")
        for _ in range(300):
            if shutdown.is_set():
                return
            await asyncio.sleep(12)