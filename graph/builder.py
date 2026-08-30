"""Incrementally derives the social graph from immutable evidence:

  reply   edge: message replies to message -> author -> author
  quote   edge: X quote-tweet reference (stored by the collector)
  mention edge: @username in text (X) / entity mentions (Telegram)
  forward edge: Telegram forward (stored by the collector)

Edges are weighted (interaction count). Parents that arrive AFTER their
replies sit in pending_edges until they show up. Watermarked: crash-safe."""

import asyncio
import json
import logging
import re
import time

from analytics_schema import analytics_connect, get_state, set_state
from graph.analysis import compute_all
from ledger import now_us

log = logging.getLogger("collector.graph")
MENTION_RE = re.compile(r"@([A-Za-z0-9_]{3,15})")


def _author_of(conn, source, external_id):
    return conn.execute("SELECT author_id FROM messages WHERE source=? AND external_id=?",
                        (source, external_id)).fetchone()


def _edge(conn, source, kind, a, b, at_us):
    if not a or not b or a == b:
        return
    conn.execute(
        "INSERT INTO graph_edges (source, kind, from_author, to_author, weight, first_us, last_us)"
        " VALUES (?,?,?,?,1,?,?)"
        " ON CONFLICT(source, kind, from_author, to_author) DO UPDATE SET"
        " weight=weight+1, last_us=max(last_us, excluded.last_us)",
        (source, kind, a, b, at_us, at_us))


def _node(conn, source, author_id, username, at_us):
    if not author_id:
        return
    conn.execute(
        "INSERT INTO graph_nodes (source, author_id, username, first_us, last_us, message_count)"
        " VALUES (?,?,?,?,?,1)"
        " ON CONFLICT(source, author_id) DO UPDATE SET username=excluded.username,"
        " last_us=max(last_us, excluded.last_us), message_count=message_count+1",
        (source, author_id, username, at_us, at_us))


def build_pass(db_path: str, limit: int = 20000) -> int:
    conn = analytics_connect(db_path)
    try:
        wm = int(get_state(conn, "graph_wm", "0"))
        rows = conn.execute(
            "SELECT id, source, external_id, author_id, author_username, text,"
            " posted_at_us, reply_to_external_id, raw_json FROM messages"
            " WHERE id>? ORDER BY id LIMIT ?", (wm, limit)).fetchall()
        if not rows:
            return 0
        conn.execute("BEGIN IMMEDIATE")
        for (mid, source, ext, author, username, text, at_us, reply_to,
             raw_json) in rows:
            raw = {}
            try:
                raw = json.loads(raw_json) if raw_json else {}
            except json.JSONDecodeError:
                pass
            _node(conn, source, author, username, at_us)

            for kind, target in (("reply", reply_to), ("quote", raw.get("quoted_id"))):
                if not target:
                    continue
                p = _author_of(conn, source, str(target))
                if p and p[0]:
                    _edge(conn, source, kind, author, p[0], at_us)
                elif author:
                    conn.execute("INSERT OR IGNORE INTO pending_edges (message_rowid, source,"
                                 " kind, external_id, at_us) VALUES (?,?,?,?,?)",
                                 (mid, source, kind, str(target), at_us))

            fwd = raw.get("forward") or {}
            if fwd.get("user_id"):
                fid = str(fwd["user_id"])
                _node(conn, source, fid, fwd.get("username"), at_us)
                _edge(conn, source, "forward", author, fid, at_us)

            names = (MENTION_RE.findall(text or "")
                     if source == "x" else raw.get("mentions") or [])
            seen = set()
            for nm in names:
                nm = nm.lstrip("@").lower()
                if not nm or nm in seen:
                    continue
                seen.add(nm)
                if username and nm == username.lower():
                    continue
                dest = conn.execute(
                    "SELECT author_id FROM authors WHERE source=? AND lower(username)=? LIMIT 1",
                    (source, nm)).fetchone()
                if dest and dest[0]:
                    _edge(conn, source, "mention", author, dest[0], at_us)

        # resolve pendings whose parent has since arrived
        for rowid, source, kind, ext, child_author in conn.execute(
                "SELECT p.message_rowid, p.source, p.kind, p.external_id, m.author_id,"
                " m.posted_at_us FROM pending_edges p"
                " JOIN messages m ON m.id=p.message_rowid").fetchall():
            p = _author_of(conn, source, ext)
            if p and p[0]:
                _edge(conn, source, kind, child_author, p[0], now_us())
                conn.execute("DELETE FROM pending_edges WHERE message_rowid=?", (rowid,))
        set_state(conn, "graph_wm", rows[-1][0])
        conn.execute("COMMIT")
        return len(rows)
    finally:
        conn.close()


async def run_graph_loop(cfg, shutdown):
    acfg = cfg.get("analytics", {})
    g = acfg.get("graph", {})
    db = acfg.get("db_path", cfg["storage"]["db_path"])
    build_wait = int(g.get("build_seconds", 30))
    recompute_s = int(g.get("recompute_minutes", 5)) * 60
    log.info("Graph service up (edges every %ss, metrics every %s min)",
             build_wait, recompute_s // 60)
    last_recompute = 0.0
    while not shutdown.is_set():
        try:
            n = await asyncio.to_thread(build_pass, db)
            if n:
                log.info("graph: processed %d messages into edges", n)
            if time.monotonic() - last_recompute >= recompute_s:
                await asyncio.to_thread(compute_all, db, g)
                last_recompute = time.monotonic()
        except Exception:
            log.exception("graph pass failed")
        for _ in range(max(5, build_wait)):
            if shutdown.is_set():
                return
            await asyncio.sleep(1)