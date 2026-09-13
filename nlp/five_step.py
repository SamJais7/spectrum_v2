"""The five-step plan, verbatim:

  1. Embed every preprocessed message (all-MiniLM-L6-v2).
  2. Semantic cache: ChromaDB cosine >= 0.95 -> inherit labels (GPU bypassed).
  3. Cache-misses -> HuggingFace GPU ensemble (LID, sarcasm, sentiment, GoEmotions).
  4. Volume/latency gate: backlog > 100 or observed batch > 2000 ms ->
     REMAINING misses go to CPU lexicon (engine_used='layer2_lexicon').
  5. Every 12 min: HDBSCAN over labeled messages; Ollama Qwen 3B writes cluster
     titles + the 100-200 word briefing and chat statistics.

The four HF models come from nlp/models/* — NOT Ollama.
Ollama is exclusively Step 5."""

import asyncio
import json
import logging
import math
import sqlite3
import threading
import time
from collections import Counter

from ledger import now_us
from nlp.engines import LexiconEmotionEngine, detect_language
from nlp.preprocessor import clean_text, filtered_tokens, tokens_json
from processed_schema import processed_connect

log = logging.getLogger("collector.five_step")
H = 3_600_000_000

_EMB = None
_EMB_LOCK = threading.Lock()


def _device():
    try:
        import torch
        return 0 if torch.cuda.is_available() else -1
    except ImportError:
        return -1


def embed_texts(texts):
    """Step 1 — all-MiniLM-L6-v2, unit-norm for cosine."""
    global _EMB
    with _EMB_LOCK:
        if _EMB is None:
            from sentence_transformers import SentenceTransformer
            log.info("loading MiniLM embedder (device=%s)…", _device())
            _EMB = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2",
                                       device=("cuda" if _device() == 0 else "cpu"))
        return _EMB.encode(list(texts), batch_size=64,
                           normalize_embeddings=True,
                           show_progress_bar=False).tolist()


class SemanticCache:
    """Step 2 — ChromaDB as a label cache (cosine space)."""

    def __init__(self, path, collection, threshold, min_hits):
        import chromadb
        self.threshold = threshold
        self.min_hits = min_hits
        self._col = chromadb.PersistentClient(path=path).get_or_create_collection(
            name=collection, metadata={"hnsw:space": "cosine"})

    def count(self):
        return self._col.count()

    def _ready(self):
        return self._col.count() >= self.min_hits

    def lookup(self, embeddings):
        """Returns list of label dicts (or None) aligned to embeddings."""
        if not self._ready():
            return [None] * len(embeddings)
        res = self._col.query(query_embeddings=embeddings, n_results=1,
                              include=["metadatas", "distances"])
        out = []
        for i in range(len(embeddings)):
            # guard: a query point can legitimately return ZERO neighbors
            # (empty filter result) — that's a miss, not an error
            if not res["metadatas"] or i >= len(res["metadatas"][0]) \
                    or not res["distances"] or i >= len(res["distances"][0]):
                out.append(None)
                continue
            m = res["metadatas"][0][i]
            d = res["distances"][0][i]
            sim = 1.0 - d
            if sim >= self.threshold and m and m.get("labeled"):
                out.append({"sentiment_label": m["sentiment"],
                            "sentiment_conf": round(m.get("sent_conf", 0.5) * sim, 3),
                            "dominant_emotion": m["emotion"],
                            "emotion_conf": round(m.get("emo_conf", 0.5) * sim, 3),
                            "is_sarcastic": int(m.get("is_sarcastic", 0)),
                            "sarcasm_conf": round(m.get("sarc_conf", 0) * sim, 3),
                            "irony_flag": int(m.get("irony", 0)),
                            "language_label": m.get("language"),
                            "rationale": f"cache hit (sim {sim:.3f})"})
            else:
                out.append(None)
        return out

    def store(self, ids, embeddings, texts, labels):
        """Add this cycle's messages to the cache for future lookups.
        All vectors are stored; unlabeled ones carry labeled=False so lookups
        skip them (they're only useful as future dedup targets after labeling)."""
        if not ids:
            return
        self._col.upsert(
            ids=[str(i) for i in ids],
            embeddings=list(embeddings),
            documents=list(texts),
            metadatas=[{"labeled": l is not None,
                        "sentiment": (l or {}).get("sentiment_label", ""),
                        "sent_conf": (l or {}).get("sentiment_conf", 0),
                        "emotion": (l or {}).get("dominant_emotion", ""),
                        "emo_conf": (l or {}).get("emotion_conf", 0),
                        "is_sarcastic": (l or {}).get("is_sarcastic", 0),
                        "sarc_conf": (l or {}).get("sarcasm_conf", 0),
                        "irony": (l or {}).get("irony_flag", 0),
                        "language": (l or {}).get("language_label", "")}
                       for l in labels])


class FiveStepPipeline:
    def __init__(self, cfg: dict):
        acfg = cfg.get("analytics", {})
        h = acfg.get("hybrid", {})
        c = acfg.get("cache", {})
        self.cfg_analytics = acfg                 # used by discovery()
        self.vault_db = cfg["storage"]["db_path"]
        self.proc_db = h.get("processed_db", "data/processed.db")
        self.batch = int(h.get("batch", 16))
        self.cap = int(h.get("queue_cap", 100))
        self.timeout_ms = int(h.get("batch_timeout_ms", 2000))
        self.cooldown_s = int(h.get("oom_cooldown_s", 300))
        self.models_cfg = h.get("models", {})
        self.pull = int(c.get("pull_chunk", 250))
        self.cache = SemanticCache(c.get("store_path", "data/vector_store"),
                                   c.get("collection", "spectrum_vectors"),
                                   float(c.get("threshold", 0.95)),
                                   int(c.get("min_hits", 500)))
        self.lex = LexiconEmotionEngine()
        self._ens = None
        self._lex_until = 0.0
        self._reason = ""
        self._ensure_columns()

    def _ensure_columns(self):
        conn = processed_connect(self.proc_db)
        try:
            try:
                conn.execute("ALTER TABLE processed_messages ADD COLUMN rationale TEXT")
            except sqlite3.OperationalError:
                pass
            conn.execute("""CREATE TABLE IF NOT EXISTS llm_summaries (
                id INTEGER PRIMARY KEY AUTOINCREMENT, summary_text TEXT NOT NULL,
                n_posts INTEGER, reason TEXT, created_at_us INTEGER NOT NULL)""")
        finally:
            conn.close()

    # -------------------------------------------------- Step 3: the ensemble

    def _ensemble(self):
        if self._ens is None:
            from nlp.models.emotion import EmotionEngine
            from nlp.models.lid import LanguageIdentifier
            from nlp.models.sarcasm import SarcasmDetector
            from nlp.models.sentiment import SentimentEngine
            dev = _device()
            log.info("loading HF transformer ensemble (device=%s)…",
                     "cuda" if dev == 0 else "cpu")
            self._ens = {
                "lid": LanguageIdentifier(self.models_cfg.get("lid", ""), dev),
                "sarcasm": SarcasmDetector(self.models_cfg.get("sarcasm", ""), dev),
                "sent": SentimentEngine(self.models_cfg.get("sentiment", ""), dev),
                "emo": EmotionEngine(self.models_cfg.get("emotion", ""), dev),
            }
        return self._ens

    def _lexicon_fields(self, text):
        lang, _ = detect_language(text)
        r = self.lex.score(text)
        emo, dom, conf = r["emotions"], r["dominant"], r["confidence"] / 100
        is_s = 1 if dom == "sarcasm" else 0
        s_conf = emo.get("sarcasm", 0) / 100
        if is_s:
            rest = {k: v for k, v in emo.items() if k != "sarcasm"}
            dom = max(rest, key=rest.get) if rest else "neutral"
            conf = rest.get(dom, 0) / 100
        sent = ("negative" if dom in ("hostile", "fear")
                else "positive" if dom in ("supportive", "excitement") else "neutral")
        return {"language_label": lang, "is_sarcastic": is_s,
                "sarcasm_conf": round(s_conf, 3), "irony_flag": 0,
                "sentiment_label": sent, "sentiment_conf": round(conf, 3),
                "dominant_emotion": dom, "emotion_conf": round(conf, 3),
                "rationale": "lexicon"}

    def _run_ensemble(self, texts):
        """One HF pass per model over the batch — LID, sarcasm, sentiment, GoEmotions."""
        ens = self._ensemble()
        out = []
        lids = ens["lid"].predict(texts)
        sarc = ens["sarcasm"].predict(texts)
        sents = ens["sent"].predict(texts)
        emos = ens["emo"].predict(texts)
        for (lang, _), (is_s, sc), (sl, sc2), (ed, ec, _d) in zip(lids, sarc, sents, emos):
            irony = 1 if (is_s and sc >= 0.80 and sl == "positive") else 0
            out.append({"language_label": lang, "is_sarcastic": is_s,
                        "sarcasm_conf": sc, "irony_flag": irony,
                        "sentiment_label": sl, "sentiment_conf": sc2,
                        "dominant_emotion": ed, "emotion_conf": ec,
                        "rationale": "layer1"})
        return out

    # ------------------------------------------------------------- the cycle

    def _cycle(self) -> dict:
        conn = processed_connect(self.proc_db)
        try:
            conn.execute(f"ATTACH DATABASE '{self.vault_db}' AS vault")
            rows = conn.execute(
                "SELECT m.id, m.source, m.author_id, m.posted_at_us, m.text"
                " FROM vault.messages m LEFT JOIN processed_messages p"
                " ON p.raw_message_id=m.id WHERE p.id IS NULL"
                " ORDER BY m.id LIMIT ?", (self.pull,)).fetchall()
            if not rows:
                return {"analyzed": 0}
            texts = [clean_text(r[4]) or "(empty)" for r in rows]

            # STEP 1 — embed everything
            embs = embed_texts(texts)
            # STEP 2 — semantic cache lookup
            cached = self.cache.lookup(embs)

            results = [None] * len(rows)
            engines = [""] * len(rows)
            for i, c in enumerate(cached):
                if c is not None:
                    results[i] = c
                    engines[i] = "cache_inherited"
            misses = [i for i in range(len(rows)) if results[i] is None]

            # STEPS 3/4 — GPU ensemble for misses, volume/latency gated.
            # The gate is re-checked EVERY batch so a slow batch or GPU fault
            # routes the REMAINING misses of this cycle to Layer 2 (spec), and
            # engine attribution is tracked per batch (never overwritten).
            if misses:
                depth = self._backlog(conn)
                use_gpu = (_device() == 0 and depth < self.cap)
                if not use_gpu and not self._reason:
                    self._reason = ("no CUDA" if _device() != 0 else
                                    f"backlog {depth}≥{self.cap}")
                for i in range(0, len(misses), self.batch):
                    chunk = [misses[j] for j in range(i, min(i + self.batch, len(misses)))]
                    batch_texts = [texts[j] for j in chunk]
                    if use_gpu and time.monotonic() < self._lex_until:
                        use_gpu = False                  # mid-cycle fallback engaged
                    batch_engine = "layer1_transformer" if use_gpu else "layer2_lexicon"
                    if use_gpu:
                        t0 = time.monotonic()
                        try:
                            res = self._run_ensemble(batch_texts)
                            dt = (time.monotonic() - t0) * 1000
                            if dt > self.timeout_ms:
                                # this batch completed on GPU (layer1) but the NEXT
                                # batches route to Layer 2 — observed-latency routing
                                self._lex_until = time.monotonic() + 60
                                self._reason = f"slow batch {dt:.0f}ms"
                                log.warning("batch %.0fms > %dms — remaining misses"
                                            " to Layer 2 for 60s", dt, self.timeout_ms)
                        except (RuntimeError, MemoryError) as e:   # CUDA OOM
                            self._lex_until = time.monotonic() + self.cooldown_s
                            self._reason = f"GPU fault: {e}"
                            log.warning("GPU fault — Layer 2 cooldown %ss",
                                        self.cooldown_s)
                            res = [self._lexicon_fields(t) for t in batch_texts]
                            batch_engine = "layer2_lexicon"
                    else:
                        res = [self._lexicon_fields(t) for t in batch_texts]
                    for j, r in zip(chunk, res):
                        results[j] = r
                        engines[j] = batch_engine

            # persist + grow the cache with ALL vectors (labeled or not)
            recs = []
            for i, (rid, source, author, posted, _raw) in enumerate(rows):
                r = results[i] or self._lexicon_fields(texts[i])
                if not engines[i]:
                    engines[i] = "layer2_lexicon"
                recs.append((rid, source, author or "", posted, texts[i],
                             tokens_json(filtered_tokens(texts[i])), engines[i],
                             r.get("language_label"), r.get("is_sarcastic", 0),
                             r.get("sarcasm_conf", 0.0), r.get("irony_flag", 0),
                             r.get("sentiment_label", "neutral"),
                             r.get("sentiment_conf", 0.0),
                             r.get("dominant_emotion", "neutral"),
                             r.get("emotion_conf", 0.0),
                             (r.get("rationale") or "")[:160], now_us()))
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany(
                "INSERT OR IGNORE INTO processed_messages (raw_message_id, source,"
                " author_id, posted_at_us, clean_text, filtered_tokens, engine_used,"
                " language_label, is_sarcastic, sarcasm_conf, irony_flag,"
                " sentiment_label, sentiment_conf, dominant_emotion, emotion_conf,"
                " rationale, processed_at_us) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                recs)
            counts = Counter(engines)
            for k, v in (("five_step_last", str(now_us())),
                         ("cache_count", str(self.cache.count())),
                         ("routing_reason", self._reason or "gpu"),
                         *tuple((f"engine_{k}", str(v)) for k, v in counts.items())):
                conn.execute("INSERT INTO hybrid_state (key,value) VALUES (?,?)"
                             " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                             (k, v))
            conn.execute("COMMIT")
            # cache grows AFTER commit — only committed labels are cached
            self.cache.store([r[0] for r in rows], embs, texts, results)
            return {"analyzed": len(rows), **dict(counts)}
        finally:
            try:
                conn.execute("DETACH DATABASE vault")
            except Exception:
                pass
            conn.close()

    def _backlog(self, conn) -> int:
        return conn.execute(
            "SELECT COUNT(*) FROM vault.messages m LEFT JOIN processed_messages p"
            " ON p.raw_message_id=m.id WHERE p.id IS NULL").fetchone()[0]

    # -------------------------------------------------------------- Step 5

    def discovery(self) -> dict:
        """HDBSCAN over recent labeled messages; Ollama titles + briefing."""
        import numpy as np
        acfg = self.cfg_analytics
        ccfg = acfg.get("clustering", {})
        lcfg = acfg.get("llm", {})
        window_us = now_us() - int(ccfg.get("window_hours", 48)) * H
        conn = processed_connect(self.proc_db)
        try:
            rows = conn.execute(
                "SELECT raw_message_id, clean_text, filtered_tokens, sentiment_label,"
                " dominant_emotion, is_sarcastic, posted_at_us FROM processed_messages"
                " WHERE posted_at_us>=? AND clean_text<>''"
                " ORDER BY posted_at_us DESC LIMIT 3000", (window_us,)).fetchall()
        finally:
            conn.close()
        if len(rows) < 60:
            return {"skipped": f"only {len(rows)} labeled posts"}

        # embed the labeled set (Step 1 machinery reused)
        embs = np.array(embed_texts([r[1] for r in rows]), dtype="float32")
        try:
            import hdbscan
            import umap
            red = umap.UMAP(n_components=5,
                            n_neighbors=min(int(ccfg.get("umap_neighbors", 15)),
                                            len(rows) - 1),
                            min_dist=0.0, metric="cosine",
                            random_state=42).fit_transform(embs)
            labels = hdbscan.HDBSCAN(
                min_cluster_size=int(ccfg.get("min_cluster_size", 15)),
                min_samples=int(ccfg.get("min_samples", 5))).fit_predict(red)
        except ImportError:
            labels = np.zeros(len(rows), dtype=int)      # one big cluster fallback

        clusters = {}
        for i, lab in enumerate(labels):
            if lab != -1:
                clusters.setdefault(int(lab), []).append(i)

        # c-TF-IDF keywords per cluster
        def ctfidf(members):
            df = Counter()
            docs = []
            for i in members:
                toks = json.loads(rows[i][2] or "[]")
                docs.append(toks)
                df.update(set(toks))
            n = len(clusters) or 1
            cnt = Counter(t for d in docs for t in d)
            tot = max(sum(cnt.values()), 1)
            return [k for k, _ in sorted(
                {t: (cnt[t] / tot) * math.log(1 + n / df[t])
                 for t in cnt}.items(), key=lambda kv: -kv[1])[:5]]

        from nlp.llm_ollama import OllamaClient
        llm = OllamaClient(lcfg) if lcfg.get("enabled", True) else None
        conn = processed_connect(self.proc_db)
        try:
            conn.execute("BEGIN IMMEDIATE")
            t = now_us()
            kept = 0
            for c, members in sorted(clusters.items(),
                                     key=lambda kv: -len(kv[1]))[:25]:
                kws = ctfidf(members)
                docs = [rows[i][1] for i in members]
                sent = Counter(rows[i][3] for i in members).most_common(1)[0][0]
                emo = Counter(rows[i][4] for i in members).most_common(1)[0][0]
                sarc = sum(rows[i][5] for i in members) / len(members)
                times = [rows[i][6] for i in members]
                rh = int(ccfg.get("velocity_recent_h", 6))
                rec_n = sum(1 for x in times if x >= now_us() - rh * H)
                pri_n = len(times) - rec_n
                vel = (rec_n / max(rh, 1)) / max(pri_n / max(
                    int(ccfg.get("window_hours", 48)) - rh, 1), 1e-6)
                title = (llm.title_cluster(kws, docs[:3]) if llm
                         else " · ".join(kws[:3]))
                conn.execute(
                    "INSERT OR REPLACE INTO topic_clusters (cluster_id, title,"
                    " top_keywords, exemplar_text, message_count, dominant_sentiment,"
                    " dominant_emotion, sarcasm_rate, velocity_score, last_updated_us)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (t // 1_000_000 + c, title, json.dumps(kws),
                     max(docs, key=len)[:400], len(members), sent, emo,
                     round(sarc, 3), round(vel, 2), t))
                kept += 1
            conn.execute("DELETE FROM topic_clusters WHERE last_updated_us<?"
                         if kept else "DELETE FROM topic_clusters", (t,))
            conn.execute("COMMIT")
        finally:
            conn.close()

        # briefing + stats (100-200 words) via Ollama
        if llm:
            make_briefing(llm, self.cfg_analytics, self.proc_db, self.vault_db,
                          reason="discovery")
        return {"clusters": kept, "noise": int(list(labels).count(-1)),
                "points": len(rows)}


# --------------------------------------------------------- briefing & stats

def _stats(conn, vault_db, window_h):
    since = now_us() - window_h * H
    stats = {"posts": conn.execute(
        "SELECT COUNT(*) FROM processed_messages WHERE posted_at_us>=?",
        (since,)).fetchone()[0]}
    if not stats["posts"]:
        return stats
    for key, col in (("sentiment", "sentiment_label"),
                     ("emotion", "dominant_emotion"),
                     ("engines", "engine_used")):
        stats[key] = {k or "unknown": n for k, n in conn.execute(
            f"SELECT {col}, COUNT(*) FROM processed_messages"
            f" WHERE posted_at_us>=? GROUP BY 1 ORDER BY 2 DESC LIMIT 6", (since,))}
    stats["sarcasm_rate"] = round(conn.execute(
        "SELECT AVG(is_sarcastic) FROM processed_messages WHERE posted_at_us>=?",
        (since,)).fetchone()[0] or 0, 3)
    buckets = dict(conn.execute(
        "SELECT (posted_at_us/3600000000)*3600000000, COUNT(*)"
        " FROM processed_messages WHERE posted_at_us>=? GROUP BY 1", (since - H,)))
    hrs = sorted(buckets)
    if len(hrs) >= 3:
        prev = [buckets[h] for h in hrs[:-1]]
        stats["velocity"] = round(buckets[hrs[-1]] / max(sum(prev) / len(prev), 1), 2)
    return stats


def make_briefing(llm, acfg, proc_db, vault_db, reason="manual"):
    """100-200 word summary + chat statistics. Returns (text, stats, n_posts)
    and stores into llm_summaries. Called by discovery() and by the dashboard's
    POST /api/brief/regenerate."""
    scfg = acfg.get("summary", {})
    window_h = int(scfg.get("window_hours", 24))
    max_posts = int(scfg.get("max_posts", 60))
    conn = processed_connect(proc_db)
    try:
        stats = _stats(conn, vault_db, window_h)
        posts = conn.execute(
            "SELECT clean_text FROM processed_messages WHERE posted_at_us>=?"
            " AND clean_text<>'' ORDER BY posted_at_us DESC LIMIT ?",
            (now_us() - window_h * H, max_posts)).fetchall()
    finally:
        conn.close()
    lines = "\n".join(f"- {p[0][:180]}" for p in reversed(posts))
    prompt = (f"Chat statistics (JSON):\n"
              f"{json.dumps(stats, ensure_ascii=False, default=str)}\n\n"
              f"Recent posts:\n{lines}\n\n"
              "Write a 100-200 word situation summary of this chat: topics, "
              "dominant mood, notable signals, acceleration. ONLY the summary text.")
    text = llm.generate(prompt, system="You are a concise intelligence analyst.",
                        max_len=400)
    w = len(text.split())
    if w < 80 or w > 230:                    # corrective retry — WITH the full data
        text2 = llm.generate(
            prompt + f"\n\n(Previous attempt was {w} words; write strictly "
                     "100-200 words.)",
            system="You are a concise intelligence analyst.", max_len=400)
        if 80 <= len(text2.split()) <= 230:
            text = text2
    conn = processed_connect(proc_db)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT INTO llm_summaries (summary_text, n_posts, reason,"
                     " created_at_us) VALUES (?,?,?,?)",
                     (text, len(posts), reason, now_us()))
        conn.execute("COMMIT")
    finally:
        conn.close()
    return text, stats, len(posts)


class FiveStepRunner:
    """Wires the cycle + the 12-minute discovery loop as async tasks."""

    def __init__(self, cfg):
        self.cfg = cfg
        self.pipe = FiveStepPipeline(cfg)      # sets its own cfg_analytics now

    async def run(self, shutdown):
        log.info("five-step pipeline up (cache=%d/%d hits before it activates, "
                 "thr=%.2f, cap=%d)", self.pipe.cache.count(),
                 int(self.pipe.cache.min_hits), self.pipe.cache.threshold,
                 self.pipe.cap)
        ccfg = self.cfg.get("analytics", {}).get("clustering", {})
        scan_s = int(ccfg.get("scan_minutes", 12)) * 60
        last_scan = 0.0
        while not shutdown.is_set():
            try:
                r = await asyncio.to_thread(self.pipe._cycle)
                if r["analyzed"]:
                    log.info("five-step: %s", r)
                    await asyncio.sleep(0.2)
                else:
                    if time.monotonic() - last_scan >= scan_s:
                        d = await asyncio.to_thread(self.pipe.discovery)
                        log.info("discovery: %s", d)
                        last_scan = time.monotonic()
                    await asyncio.sleep(3)
            except Exception:
                log.exception("five-step cycle failed")
                await asyncio.sleep(10)