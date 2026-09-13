"""Analyst dashboard API. Read-only against the vault. Times on the wire are
epoch MILLISECONDS. Optional auth: DASHBOARD_TOKEN env -> Authorization: Bearer.

    uvicorn dashboard.server:app --host 127.0.0.1 --port 8080
"""
import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

import yaml
from fastapi import FastAPI, HTTPException, Query, Request, Body
from fastapi.staticfiles import StaticFiles

from analytics_schema import analytics_connect, ensure_schema
from graph.analysis import compute_all
from graph.builder import build_pass
from ledger import now_us

H = 3_600_000_000
EMOTIONS = ["supportive", "hostile", "sarcasm", "fear", "excitement", "neutral"]

with open("config.yaml", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)
DB = CFG["storage"]["db_path"]
TOKEN = os.getenv("DASHBOARD_TOKEN", "")
US = 1000  # ms -> us

app = FastAPI(title="Collector Command Center")
_conn = analytics_connect(DB)


def _ro(q, args=()):
    return _conn.execute(q, args).fetchall()


@app.middleware("http")
async def auth(request: Request, call_next):
    if TOKEN and request.url.path.startswith("/api"):
        if request.headers.get("authorization") != f"Bearer {TOKEN}":
            raise HTTPException(401, "bad or missing token")
    return await call_next(request)


def _resolve_times(from_ms, to_ms, from_alt, to_alt, f_alt):
    """Fallback handler for missing/differently-named frontend time parameters."""
    cur_ms = int(time.time() * 1000)
    resolved_to = to_ms if to_ms is not None else (to_alt if to_alt is not None else cur_ms + 86_400_000)
    resolved_from = from_ms if from_ms is not None else (from_alt if from_alt is not None else (f_alt if f_alt is not None else 0))
    return int(resolved_from), int(resolved_to)


def _where(from_ms, to_ms, platform, emotion, location, language, q):
    w = ["m.posted_at_us>=?", "m.posted_at_us<?"]
    a = [from_ms * US, to_ms * US]
    if platform:
        w.append("m.source=?")
        a.append(platform)
    if emotion:
        w.append("a.dominant_emotion=?")
        a.append(emotion)
    if location:
        w.append("a.author_location=?")
        a.append(location)
    if language:
        w.append("a.author_language=?")
        a.append(language)
    if q:
        w.append("m.id IN (SELECT rowid FROM messages_fts WHERE messages_fts MATCH ?)")
        a.append(q)
    return "LEFT JOIN message_analytics a ON a.message_rowid=m.id", " AND ".join(w), a


@app.get("/api/meta")
def meta():
    row = _ro("SELECT MIN(posted_at_us), MAX(posted_at_us) FROM messages")[0]
    return {
        "min_ms": row[0] // US if row[0] else None,
        "max_ms": row[1] // US if row[1] else None,
        "emotions": EMOTIONS,
        "platforms": [r[0] for r in _ro("SELECT DISTINCT source FROM messages")],
        "locations": [r[0] for r in _ro(
            "SELECT DISTINCT author_location FROM message_analytics "
            "WHERE author_location IS NOT NULL ORDER BY 1 LIMIT 200")],
        "languages": [r[0] for r in _ro(
            "SELECT DISTINCT author_language FROM message_analytics "
            "WHERE author_language IS NOT NULL ORDER BY 1 LIMIT 100")],
    }


@app.get("/api/overview")
def overview(from_ms: Optional[int] = None, 
             to_ms: Optional[int] = None,
             from_: Optional[int] = Query(None, alias="from"),
             to: Optional[int] = Query(None, alias="to"),
             f: Optional[int] = Query(None, alias="f"),
             platform: str = "", emotion: str = "",
             location: str = "", language: str = "", q: str = ""):
    from_ms, to_ms = _resolve_times(from_ms, to_ms, from_, to, f)
    joins, where, args = _where(from_ms, to_ms, platform, emotion, location, language, q)
    tot = _ro(f"SELECT COUNT(*), COUNT(DISTINCT m.author_id) FROM messages m {joins}"
              f" WHERE {where}", args)[0]
    mix = _ro(f"SELECT a.dominant_emotion, COUNT(*) FROM messages m {joins} WHERE {where} "
              f"GROUP BY 1 ORDER BY 2 DESC", args)
    alerts = _ro("SELECT COUNT(*) FROM viral_alerts WHERE triggered_at_us>=?",
                 (now_us() - 24 * H,))[0][0]
    return {"posts": tot[0], "users": tot[1],
            "sentiment_mix": {e or "unknown": n for e, n in mix},
            "alerts_24h": alerts}


@app.get("/api/timeline")
def timeline(from_ms: Optional[int] = None, 
             to_ms: Optional[int] = None,
             from_: Optional[int] = Query(None, alias="from"),
             to: Optional[int] = Query(None, alias="to"),
             f: Optional[int] = Query(None, alias="f"),
             platform: str = "", emotion: str = "",
             location: str = "", language: str = "", q: str = ""):
    from_ms, to_ms = _resolve_times(from_ms, to_ms, from_, to, f)
    span = max(to_ms - from_ms, 60_000)
    for cand in (60_000, 900_000, 3_600_000, 21_600_000, 86_400_000):
        bucket = cand
        if span / cand <= 240:
            break
    joins, where, args = _where(from_ms, to_ms, platform, emotion, location, language, q)
    rows = _ro(f"SELECT (m.posted_at_us/{bucket*US})*{bucket*US}, m.source, COUNT(*) "
               f"FROM messages m {joins} WHERE {where} GROUP BY 1,2 ORDER BY 1", args)
    series = {}
    for b, src, n in rows:
        series.setdefault(b // US, {})[src] = n
    buckets = sorted(series)
    return {"bucket_ms": bucket, "buckets": buckets,
            "series": {s: [series[b].get(s, 0) for b in buckets]
                       for s in ("x", "telegram")}}


@app.get("/api/graph")
def graph(from_ms: Optional[int] = None, 
          to_ms: Optional[int] = None,
          from_: Optional[int] = Query(None, alias="from"),
          to: Optional[int] = Query(None, alias="to"),
          f: Optional[int] = Query(None, alias="f"),
          limit: int = 300, platform: str = "",
          emotion: str = "", location: str = "", language: str = "", q: str = ""):
    from_ms, to_ms = _resolve_times(from_ms, to_ms, from_, to, f)
    latest = _ro("SELECT MAX(computed_at_us) FROM graph_metrics")[0][0]
    if not latest:
        return {"nodes": [], "edges": []}
    filt, args = ("AND gm.source=?", [platform]) if platform else ("", [])
    nodes = _ro(f"SELECT gm.source, gm.author_id, gm.in_w, gm.out_w, gm.pagerank,"
                f" gm.betweenness, gm.community, gm.bridge, gn.username,"
                f" c.suspicion, c.flagged FROM graph_metrics gm"
                f" JOIN graph_nodes gn ON gn.source=gm.source AND gn.author_id=gm.author_id"
                f" LEFT JOIN communities c ON c.source=gm.source AND c.community=gm.community"
                f" WHERE gm.computed_at_us=? {filt} ORDER BY gm.pagerank DESC LIMIT ?",
                [latest] + args + [limit])
    ids = {(s, a) for s, a, *_ in nodes}

    # per-author dominant emotion & location WITHIN the current time window
    def _mode(col):
        rows = _ro(f"SELECT m.source, m.author_id, a.{col}, COUNT(*) c,"
                   f" ROW_NUMBER() OVER (PARTITION BY m.source, m.author_id"
                   f" ORDER BY COUNT(*) DESC) rn FROM messages m"
                   f" JOIN message_analytics a ON a.message_rowid=m.id"
                   f" WHERE m.posted_at_us>=? AND m.posted_at_us<? GROUP BY 1,2,3",
                   (from_ms * US, to_ms * US))
        return {(s, a): v for s, a, v, _, rn in rows if rn == 1}
    emo, loc = _mode("dominant_emotion"), _mode("author_location")

    edges = []
    for src, kind, fa, ta, w in _ro("SELECT source, kind, from_author, to_author, weight "
                                    "FROM graph_edges WHERE last_us>=?",
                                    (from_ms * US,)):
        if (src, fa) in ids and (src, ta) in ids:
            edges.append({"from": f"{src}:{fa}", "to": f"{src}:{ta}",
                          "kind": kind, "weight": w})
        if len(edges) >= 6000:
            break
    return {
        "nodes": [{"id": f"{s}:{a}", "source": s, "author_id": a, "username": u,
                   "in_w": iw, "out_w": ow, "pagerank": pr, "betweenness": bt,
                   "community": c, "bridge": br, "suspicion": su, "flagged": fl,
                   "emotion": emo.get((s, a)), "location": loc.get((s, a))}
                  for s, a, iw, ow, pr, bt, c, br, u, su, fl in nodes],
        "edges": edges}

_graph_refresh_lock = threading.Lock()


def _collector_graph_alive() -> bool:
    """Heartbeat check: is the collector's edge-building loop running?
    If yes, our refresh only runs the analysis pass — edges are ≤30s old,
    and building them from two processes at once double-counts weights."""
    try:
        row = _conn.execute("SELECT value FROM analytics_state "
                            "WHERE name='graph_loop_last'").fetchone()
        return bool(row) and (time.time() - float(row[0])) < 90
    except sqlite3.Error:
        return False


@app.post("/api/graph/refresh")
def graph_refresh():
    """Compute the social graph NOW (normally on a 5-minute timer) so the
    map is populated the moment the dashboard opens. Safe alongside the
    collector: analysis is an atomic delete+insert; concurrent calls
    serialize on a lock; edge building only happens when the collector's
    graph loop is not alive."""
    if not _graph_refresh_lock.acquire(blocking=False):
        return {"status": "already-running"}
    try:
        ensure_schema(DB)
        t0 = time.monotonic()
        built = 0
        if not _collector_graph_alive():
            while True:                  # collector stopped: bring edges current too
                k = build_pass(DB)
                built += k
                if k == 0:
                    break
        compute_all(DB, CFG.get("analytics", {}).get("graph", {}))
        return {"status": "ok", "edge_messages_processed": built,
                "collector_graph_loop": _collector_graph_alive(),
                "seconds": round(time.monotonic() - t0, 1)}
    finally:
        _graph_refresh_lock.release()


@app.get("/api/trace")
def trace(source: str, external_id: str = "", author_id: str = "", limit: int = 400):
    if external_id:
        root = _ro("SELECT id, source, external_id, author_id, author_username,"
                   " posted_at_us, substr(text,1,160) FROM messages"
                   " WHERE source=? AND external_id=?", (source, external_id))
    else:
        root = _ro("SELECT m.id, m.source, m.external_id, m.author_id, m.author_username,"
                   " m.posted_at_us, substr(m.text,1,160) FROM messages m"
                   " WHERE m.source=? AND m.author_id=? AND m.id IN"
                   " (SELECT id FROM messages WHERE source=? AND reply_to_external_id IS NULL)"
                   " ORDER BY m.posted_at_us DESC LIMIT 1", (source, author_id, source))
    if not root:
        raise HTTPException(404, "root message not found")
    root = root[0]
    out, frontier, edges = [root], [root], []
    while frontier and len(out) < limit:
        nxt = []
        for p in frontier:
            kids = _ro("SELECT id, source, external_id, author_id, author_username,"
                       " posted_at_us, substr(text,1,160) FROM messages"
                       " WHERE source=? AND reply_to_external_id=? LIMIT 50",
                       (p[1], p[2]))
            for k in kids:
                out.append(k)
                edges.append({"from": p[0], "to": k[0], "at_us": k[5]})
            nxt.extend(kids)
        frontier = nxt
    return {"root": root[0], "count": len(out),
            "nodes": [{"mid": r[0], "external_id": r[2], "author_id": r[3],
                       "username": r[4], "at_ms": r[5] // US, "text": r[6]} for r in out],
            "edges": [{"from": e["from"], "to": e["to"], "at_ms": e["at_us"] // US}
                      for e in edges]}


@app.get("/api/topics")
def topics():
    now = now_us()
    rows = _ro("SELECT topic_key, label FROM topics WHERE active=1")
    counts = {}
    for k, h, n in _ro("SELECT topic_key, hour_us, n FROM topic_hourly WHERE hour_us>=?",
                       (now - 24 * H,)):
        counts.setdefault(k, {})[h] = n
    cur_h = (now // H) * H
    alerted = {r[0] for r in _ro("SELECT DISTINCT topic_key FROM viral_alerts "
                                 "WHERE triggered_at_us>=?", (now - 24 * H,))}
    out = []
    for k, label in rows:
        b = counts.get(k, {})
        vol = b.get(cur_h, 0)
        prev = [n for h, n in b.items() if h < cur_h]
        base = sum(prev) / len(prev) if prev else 0.0
        velocity = vol / max(base, 0.5)
        spark = [b.get(cur_h - i * H, 0) for i in range(23, -1, -1)]
        out.append({"key": k, "label": label, "volume": vol,
                    "velocity": round(velocity, 1), "spark": spark,
                    "alert": k in alerted})
    out.sort(key=lambda t: -t["volume"])
    return {"topics": out[:10]}


@app.get("/api/topic/{key}")
def topic_detail(key: str):
    now = now_us()
    since = now - 48 * H
    hourly = _ro("SELECT hour_us, n FROM topic_hourly WHERE topic_key=? ORDER BY hour_us "
                 "DESC LIMIT 48", (key,))
    vips = _ro("SELECT m.author_id, m.author_username, COUNT(*) c, gm.pagerank, gm.in_w,"
               " gm.bridge FROM message_topics mt JOIN messages m ON m.id=mt.message_rowid"
               " LEFT JOIN graph_metrics gm ON gm.source=m.source AND gm.author_id=m.author_id"
               " WHERE mt.topic_key=? AND m.posted_at_us>=?"
               " GROUP BY 1,2 ORDER BY c DESC LIMIT 8", (key, since))

    def breakdown(col):
        return [{"label": r[0] or "unknown", "count": r[1]} for r in _ro(
            f"SELECT a.{col}, COUNT(*) FROM message_topics mt"
            f" JOIN messages m ON m.id=mt.message_rowid"
            f" JOIN message_analytics a ON a.message_rowid=m.id"
            f" WHERE mt.topic_key=? AND m.posted_at_us>=? AND a.{col} IS NOT NULL"
            f" GROUP BY 1 ORDER BY 2 DESC LIMIT 8", (key, since))]
    return {"key": key,
            "hourly": [{"t_ms": h // US, "n": n} for h, n in reversed(hourly)],
            "vips": [{"author_id": a, "username": u, "posts": c,
                      "pagerank": pr or 0, "in_w": iw or 0, "bridge": br or 0}
                     for a, u, c, pr, iw, br in vips],
            "age": breakdown("author_age_bucket"),
            "location": breakdown("author_location"),
            "language": breakdown("author_language")}


@app.get("/api/alerts")
def alerts():
    return {"alerts": [{"topic_key": k, "label": lb, "at_ms": t // US,
                        "prev": p, "cur": c, "ratio": r}
                       for k, lb, t, p, c, r in _ro(
                           "SELECT v.topic_key, t.label, v.triggered_at_us, v.prev_rate,"
                           " v.new_rate, v.ratio FROM viral_alerts v"
                           " LEFT JOIN topics t ON t.topic_key=v.topic_key"
                           " ORDER BY v.id DESC LIMIT 20")]}

# ---------------- hybrid pipeline: vectors, clusters, LLM (blueprint phases 4-6) ----
_proc_conn = None
_chroma = None
_sem_lock = threading.Lock()


def _processed():
    global _proc_conn
    if _proc_conn is None:
        from processed_schema import processed_connect
        _proc_conn = processed_connect(
            CFG["analytics"]["hybrid"].get("processed_db", "data/processed.db"))
    return _proc_conn


def _store():
    global _chroma
    if _chroma is None:
        from nlp.embeddings import ChromaStore
        e = CFG.get("analytics", {}).get("embeddings", {})
        _chroma = ChromaStore(e.get("store_path", "data/vector_store"),
                              e.get("collection", "spectrum_vectors"))
    return _chroma


def _store_query(q, k, where):
    for attempt in (1, 2):                    # collector process writes; retry on lock
        try:
            with _sem_lock:
                return _store().query(q, k=k, where=where)
        except Exception as e:
            if attempt == 2:
                raise HTTPException(503, f"vector store busy/unavailable: {e}")
            time.sleep(0.4)


@app.get("/api/search/semantic")
def semantic_search(q: str, k: int = 20, source: str = "", sentiment: str = "",
                    from_ms: int = 0, to_ms: int = 0):
    if not q:
        raise HTTPException(400, "q required")
    if not CFG.get("analytics", {}).get("embeddings", {}).get("enabled", True):
        raise HTTPException(503, "embeddings disabled in config")
    conds = []
    if source:
        conds.append({"source": {"$eq": source}})
    if sentiment:
        conds.append({"sentiment": {"$eq": sentiment}})
    if from_ms and to_ms:
        conds.append({"posted_at_us": {"$gte": from_ms * 1000, "$lte": to_ms * 1000}})
    where = conds[0] if len(conds) == 1 else ({"$and": conds} if conds else None)
    t0 = time.monotonic()
    hits = _store_query(q, int(k), where)
    return {"query": q, "ms": round((time.monotonic() - t0) * 1000, 1), "hits": hits}


@app.get("/api/topics/unsupervised")
def unsupervised_topics():
    rows = _processed().execute(
        "SELECT cluster_id, title, top_keywords, message_count, dominant_sentiment,"
        " dominant_emotion, sarcasm_rate, velocity_score, exemplar_text"
        " FROM topic_clusters ORDER BY velocity_score DESC, message_count DESC"
        " LIMIT 24").fetchall()
    out = []
    for r in rows:
        try:
            kws = json.loads(r[2])
        except Exception:
            kws = []
        out.append({"cluster_id": r[0], "title": r[1], "keywords": kws,
                    "messages": r[3], "sentiment": r[4], "emotion": r[5],
                    "sarcasm_rate": r[6], "velocity": r[7], "exemplar": r[8]})
    return {"clusters": out}


@app.post("/api/intelligence/summary")
def intelligence_summary(body: dict = Body(...)):
    q = body.get("q", "")
    if not q:
        raise HTTPException(400, "q required")
    llm_cfg = CFG.get("analytics", {}).get("llm", {})
    if not llm_cfg.get("enabled", True):
        raise HTTPException(503, "llm disabled in config")
    from nlp.llm_ollama import OllamaClient
    hits = _store_query(q, 20, None)
    t0 = time.monotonic()
    brief = OllamaClient(llm_cfg).brief(q, hits)
    return {"query": q, "brief": brief,
            "llm_ms": round((time.monotonic() - t0) * 1000), "citations": hits[:20]}


@app.get("/api/hybrid/status")
def hybrid_status():
    conn = _processed()

    def st(k, d=None):
        row = conn.execute("SELECT value FROM hybrid_state WHERE key=?", (k,)).fetchone()
        return row[0] if row else d
    total = conn.execute("SELECT COUNT(*) FROM processed_messages").fetchone()[0]
    engines = {k or "unknown": n for k, n in conn.execute(
        "SELECT engine_used, COUNT(*) FROM processed_messages GROUP BY 1")}
    l1 = engines.get("layer1_transformer", 0)
    cached = engines.get("cache_inherited", 0)
    l2 = engines.get("layer2_lexicon", 0)
    return {"processed": total, "layer1": l1, "layer2": l2,
            "cache_inherited": cached,
            "fallback_ratio": round(l2 / total, 3) if total else 0.0,
            "cache_hit_ratio": round(cached / total, 3) if total else 0.0,
            "cache_vectors": int(st("cache_count", 0) or 0),
            "routing": st("routing_reason", "gpu"),
            "last_cycle": st("five_step_last", None)}
# ---------------- five-step pipeline: briefing + chat statistics --------------------

@app.get("/api/brief")
def brief():
    """Latest 100-200 word LLM summary + exact chat statistics."""
    from nlp.five_step import _stats
    conn = _processed()
    stats = _stats(conn, DB, 24)
    row = conn.execute("SELECT summary_text, n_posts, reason, created_at_us"
                       " FROM llm_summaries ORDER BY id DESC LIMIT 1").fetchone()
    return {"stats": stats, "summary": row[0] if row else None,
            "n_posts": row[1] if row else 0, "reason": row[2] if row else None,
            "at_ms": (row[3] // 1000) if row else None}


@app.post("/api/brief/regenerate")
def brief_regen():
    """Force a fresh briefing now (Ollama, ~30-90s)."""
    if not CFG.get("analytics", {}).get("llm", {}).get("enabled", True):
        raise HTTPException(503, "llm disabled in config")
    from nlp.five_step import make_briefing
    from nlp.llm_ollama import OllamaClient
    acfg = CFG.get("analytics", {})
    text, stats, n = make_briefing(OllamaClient(acfg.get("llm", {})),
                                   acfg, acfg.get("hybrid", {}).get("processed_db",
                                                                    "data/processed.db"),
                                   DB)
    return {"summary": text, "stats": stats, "n_posts": n}

app.mount("/", StaticFiles(directory=str(Path(__file__).parent / "static"), html=True),
          name="static")