"""Embedding pipeline + ChromaDB vector store (blueprint §5).

Chain: controller writes processed_messages → this loop embeds clean_text
into the persistent Chroma collection → clustering reads embeddings back.
Watermark-driven, idempotent (upsert), crash-safe."""

import asyncio
import logging
import threading

from processed_schema import processed_connect

log = logging.getLogger("collector.embeddings")

_LOCK = threading.Lock()
_MODEL = None
_DEVICE = None


def _device():
    global _DEVICE
    if _DEVICE is None:
        try:
            import torch
            _DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
        except ImportError:
            _DEVICE = "cpu"
    return _DEVICE


def get_embedder():
    global _MODEL
    with _LOCK:
        if _MODEL is None:
            from sentence_transformers import SentenceTransformer
            log.info("loading embedding model on %s (first use)…", _device())
            _MODEL = SentenceTransformer(
                "sentence-transformers/all-MiniLM-L6-v2", device=_device())
        return _MODEL


def embed_texts(texts):
    return get_embedder().encode(list(texts), batch_size=64,
                                 show_progress_bar=False,
                                 normalize_embeddings=True).tolist()


class ChromaStore:
    def __init__(self, path, collection="spectrum_vectors"):
        import chromadb
        self._client = chromadb.PersistentClient(path=path)
        self._col = self._client.get_or_create_collection(
            name=collection, metadata={"hnsw:space": "cosine"})

    def count(self):
        return self._col.count()

    def upsert(self, ids, embeddings, documents, metadatas):
        self._col.upsert(ids=[str(i) for i in ids], embeddings=embeddings,
                         documents=documents, metadatas=metadatas)

    def query(self, text, k=20, where=None):
        q = embed_texts([text])[0]
        res = self._col.query(query_embeddings=[q], n_results=k, where=where,
                              include=["documents", "metadatas", "distances"])
        hits = []
        for i in range(len(res["ids"][0])):
            m = res["metadatas"][0][i] or {}
            hits.append({"raw_message_id": int(res["ids"][0][i]),
                         "clean_text": res["documents"][0][i],
                         "similarity": round(1 - res["distances"][0][i], 3), **m})
        return hits

    def get_window(self, posted_at_us_gte):
        return self._col.get(
            where={"posted_at_us": {"$gte": posted_at_us_gte}},
            include=["documents", "metadatas", "embeddings"])


def _embed_cycle(proc_db, store, batch) -> int:
    conn = processed_connect(proc_db)
    try:
        row = conn.execute("SELECT value FROM hybrid_state WHERE name='embedded_max'").fetchone()
        wm = int(row[0]) if row else 0
        rows = conn.execute(
            "SELECT raw_message_id, clean_text, source, author_id, posted_at_us,"
            " sentiment_label, dominant_emotion, is_sarcastic, engine_used"
            " FROM processed_messages WHERE raw_message_id>? AND clean_text<>''"
            " ORDER BY raw_message_id LIMIT ?", (wm, batch)).fetchall()
        if not rows:
            return 0
        ids = [r[0] for r in rows]
        embs = embed_texts([r[1] for r in rows])
        metas = [{"source": r[2], "author_id": r[3] or "", "posted_at_us": r[4],
                  "sentiment": r[5], "emotion": r[6], "is_sarcastic": int(r[7] or 0),
                  "engine": r[8]} for r in rows]
        store.upsert(ids, embs, [r[1] for r in rows], metas)
        conn.execute("INSERT INTO hybrid_state (key,value) VALUES ('embedded_max',?)"
                     " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                     (str(max(ids)),))
        conn.execute("INSERT INTO hybrid_state (key,value) VALUES ('embedded_count',?)"
                     " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                     (str(store.count()),))
        return len(rows)
    finally:
        conn.close()


async def run_embedding_loop(cfg, shutdown):
    acfg = cfg.get("analytics", {})
    e = acfg.get("embeddings", {})
    if not e.get("enabled", True):
        return
    proc_db = acfg.get("hybrid", {}).get("processed_db", "data/processed.db")
    store = ChromaStore(e.get("store_path", "data/vector_store"),
                        e.get("collection", "spectrum_vectors"))
    log.info("embedding loop up (device=%s, count=%d)", _device(), store.count())
    while not shutdown.is_set():
        try:
            n = await asyncio.to_thread(_embed_cycle, proc_db, store,
                                        int(e.get("batch", 64)))
            if n:
                log.info("embeddings: +%d (total %d)", n, store.count())
                await asyncio.sleep(0.2)
            else:
                await asyncio.sleep(5)
        except Exception:
            log.exception("embedding cycle failed")
            await asyncio.sleep(10)