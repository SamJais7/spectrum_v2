"""Unsupervised discovery (blueprint §6): UMAP -> HDBSCAN -> c-TF-IDF ->
velocity + Ollama titling into processed.db:topic_clusters."""

import asyncio
import json
import logging
import math
from collections import Counter

from ledger import now_us
from nlp.llm_ollama import OllamaClient
from processed_schema import processed_connect

log = logging.getLogger("collector.clustering")
H = 3_600_000_000


def _ensure_sig_column(conn):
    try:                                   # DBs created before this phase
        conn.execute("ALTER TABLE topic_clusters ADD COLUMN sig TEXT")
    except Exception:
        pass


def _ctfidf(cluster_tokens: dict) -> dict:
    """Class-based TF-IDF — each cluster is one document."""
    n = len(cluster_tokens)
    df = Counter()
    for toks in cluster_tokens.values():
        df.update(set(toks))
    out = {}
    for c, toks in cluster_tokens.items():
        cnt = Counter(toks)
        tot = max(sum(cnt.values()), 1)
        scores = {t: (cnt[t] / tot) * math.log(1 + n / df[t]) for t in cnt}
        out[c] = sorted(scores.items(), key=lambda kv: -kv[1])[:5]
    return out


def run_cluster_scan(cfg: dict) -> dict:
    acfg = cfg.get("analytics", {})
    ccfg = acfg.get("clustering", {})
    hcfg = acfg.get("hybrid", {})
    llm_cfg = acfg.get("llm", {})
    proc_db = hcfg.get("processed_db", "data/processed.db")
    window_h = int(ccfg.get("window_hours", 48))
    window_us = now_us() - window_h * H
    min_size = int(ccfg.get("min_cluster_size", 15))
    min_samples = int(ccfg.get("min_samples", 5))
    recent_h = int(ccfg.get("velocity_recent_h", 6))

    import numpy as np                    # ImportError surfaces to the loop
    import hdbscan
    import umap

    from nlp.embeddings import ChromaStore
    ecfg = acfg.get("embeddings", {})
    store = ChromaStore(ecfg.get("store_path", "data/vector_store"),
                        ecfg.get("collection", "spectrum_vectors"))
    res = store.get_window(window_us)
    ids = [int(i) for i in res["ids"]]
    if len(ids) < max(60, min_size * 3):
        return {"skipped": f"window too small ({len(ids)} pts)", "clusters": 0}

    X = np.array(res["embeddings"], dtype=np.float32)
    metas = res["metadatas"]
    red = umap.UMAP(n_components=5, n_neighbors=int(ccfg.get("umap_neighbors", 15)),
                    min_dist=0.0, metric="cosine", random_state=42).fit_transform(X)
    labels = hdbscan.HDBSCAN(min_cluster_size=min_size, min_samples=min_samples,
                             metric="euclidean").fit_predict(red)

    clusters = {}
    for idx, lab in enumerate(labels):
        if lab != -1:                                    # -1 = noise (blueprint)
            clusters.setdefault(int(lab), []).append(idx)
    if len(clusters) > 50:                               # over-clustering guard
        log.warning("clustering: %d clusters — keeping top 25 by size", len(clusters))
        clusters = dict(sorted(clusters.items(), key=lambda kv: -len(kv[1]))[:25])

    conn = processed_connect(proc_db)
    try:
        _ensure_sig_column(conn)
        tokens_by_id = {}
        for i in range(0, len(ids), 900):                # sqlite param limit safety
            chunk = ids[i:i + 900]
            tokens_by_id.update({r[0]: json.loads(r[1] or "[]") for r in conn.execute(
                "SELECT raw_message_id, filtered_tokens FROM processed_messages"
                f" WHERE raw_message_id IN ({','.join('?' * len(chunk))})", chunk)})

        cluster_tokens = {c: [t for i in idxs for t in tokens_by_id.get(ids[i], [])]
                          for c, idxs in clusters.items()}
        kws = _ctfidf(cluster_tokens)
        llm = OllamaClient(llm_cfg) if llm_cfg.get("enabled", True) else None

        conn.execute("BEGIN IMMEDIATE")
        t = now_us()
        seen_sigs = set()
        for c, idxs in clusters.items():
            top_kws = [k for k, _ in kws.get(c, [])]
            sig = "|".join(sorted(w.lower() for w in top_kws))
            seen_sigs.add(sig)
            posts = [metas[i] for i in idxs]
            times = [m["posted_at_us"] for m in posts]
            recent_n = sum(1 for x in times if x >= now_us() - recent_h * H)
            prior_n = len(times) - recent_n
            v = (recent_n / max(recent_h, 1)) / max(prior_n / max(window_h - recent_h, 1), 1e-6)
            sent = Counter(m.get("sentiment") for m in posts).most_common(1)[0][0] or "neutral"
            emo = Counter(m.get("emotion") for m in posts).most_common(1)[0][0] or "neutral"
            sarc = sum(1 for m in posts if m.get("is_sarcastic")) / len(posts)
            docs = [res["documents"][i] for i in idxs]
            exemplar = max(docs, key=len)[:400]
            existing = conn.execute("SELECT cluster_id FROM topic_clusters WHERE sig=?",
                                    (sig,)).fetchone()
            if existing:                                  # same topic as last scan: keep title
                conn.execute("UPDATE topic_clusters SET top_keywords=?, exemplar_text=?,"
                             " message_count=?, dominant_sentiment=?, dominant_emotion=?,"
                             " sarcasm_rate=?, velocity_score=?, last_updated_us=?"
                             " WHERE cluster_id=?",
                             (json.dumps(top_kws), exemplar, len(idxs), sent, emo,
                              round(sarc, 3), round(v, 2), t, existing[0]))
            else:
                title = (llm.title_cluster(top_kws, docs[:3]) if llm
                         else " · ".join(top_kws[:3]))
                conn.execute("INSERT INTO topic_clusters (title, top_keywords,"
                             " exemplar_text, message_count, dominant_sentiment,"
                             " dominant_emotion, sarcasm_rate, velocity_score,"
                             " last_updated_us, sig) VALUES (?,?,?,?,?,?,?,?,?,?)",
                             (title, json.dumps(top_kws), exemplar, len(idxs), sent, emo,
                              round(sarc, 3), round(v, 2), t, sig))
        if seen_sigs:                                      # retire clusters gone from window
            qm = ",".join("?" * len(seen_sigs))
            gone = conn.execute(f"SELECT COUNT(*) FROM topic_clusters WHERE sig IS NULL"
                                f" OR sig NOT IN ({qm})", tuple(seen_sigs)).fetchone()[0]
            conn.execute(f"DELETE FROM topic_clusters WHERE sig IS NULL"
                         f" OR sig NOT IN ({qm})", tuple(seen_sigs))
        else:
            gone = conn.execute("SELECT COUNT(*) FROM topic_clusters").fetchone()[0]
            conn.execute("DELETE FROM topic_clusters")
        conn.execute("COMMIT")
        return {"clusters": len(clusters), "noise": int(list(labels).count(-1)),
                "points": len(ids), "retired": gone}
    finally:
        conn.close()


async def run_clustering_loop(cfg, shutdown):
    acfg = cfg.get("analytics", {})
    ccfg = acfg.get("clustering", {})
    if not ccfg.get("enabled", True):
        return
    if not acfg.get("embeddings", {}).get("enabled", True):
        log.warning("clustering requires embeddings — skipping")
        return
    wait = int(ccfg.get("scan_minutes", 12)) * 60
    log.info("clustering loop up (every %s min, %sh window)",
             wait // 60, ccfg.get("window_hours", 48))
    while not shutdown.is_set():
        try:
            r = await asyncio.to_thread(run_cluster_scan, cfg)
            log.info("clustering scan: %s", r)
        except ImportError as e:
            log.error("clustering deps missing (%s) — pip install umap-learn hdbscan;"
                      " clustering disabled this session", e)
            return
        except Exception:
            log.exception("clustering scan failed")
        for _ in range(max(60, wait)):
            if shutdown.is_set():
                return
            await asyncio.sleep(1)