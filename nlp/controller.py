"""Two-layer hybrid cascade (blueprint §3).

ARCHITECTURAL NOTE: message loss is impossible here by construction — the
vault commits evidence before analytics runs, and this controller drains the
vault by watermark (raw_message_id). Layer-1/Layer-2 is therefore a
QUALITY/LATENCY tradeoff, not a durability mechanism.

Routing rules (implementable semantics of the blueprint):
  * Layer 2 while: unanalyzed backlog >= queue_cap, OR cooldown active
    (after CUDA OOM: full cooldown; after slow batch: 60s probation),
    OR CUDA unavailable, OR force_layer2.
  * 'batch_timeout_ms' cannot interrupt a running kernel — it routes the
    NEXT batches based on observed latency.
"""

import asyncio
import json
import logging
import time

from analytics_schema import analytics_connect  # noqa: F401 (consistency)
from ledger import now_us
from nlp.engines import LexiconEmotionEngine, detect_language
from nlp.preprocessor import clean_text, filtered_tokens, tokens_json
from storage.processed_schema import ensure_processed_schema, processed_connect

log = logging.getLogger("collector.hybrid")


class HybridController:
    def __init__(self, cfg: dict):
        acfg = cfg.get("analytics", {})
        h = acfg.get("hybrid", {})
        self.vault_db = cfg["storage"]["db_path"]
        self.proc_db = h.get("processed_db", "data/processed.db")
        self.pull = int(h.get("pull_chunk", 400))
        self.batch = int(h.get("batch", 16))
        self.cap = int(h.get("queue_cap", 100))
        self.timeout_ms = int(h.get("batch_timeout_ms", 2000))
        self.cooldown_s = int(h.get("oom_cooldown_s", 300))
        self.force2 = bool(h.get("force_layer2", False))
        self.irony_thr = float(h.get("irony_threshold", 0.80))
        self.models = h.get("models", {})
        self._ensemble = None
        self._lex = LexiconEmotionEngine()
        self._lex_until = 0.0
        self._lex_reason = ""
        ensure_processed_schema(self.proc_db)

    # ------------------------------------------------------------- routing

    def _gpu_available(self) -> bool:
        if self.force2:
            return False
        try:
            import torch
            return torch.cuda.is_available()
        except ImportError:
            return False

    def _get_ensemble(self):
        if self._ensemble is None:
            from nlp.models.emotion import EmotionEngine
            from nlp.models.lid import LanguageIdentifier
            from nlp.models.sarcasm import SarcasmDetector
            from nlp.models.sentiment import SentimentEngine
            dev = 0
            log.info("loading Layer-1 transformer ensemble on CUDA (first use)…")
            self._ensemble = {
                "lid": LanguageIdentifier(self.models.get("lid", ""), dev),
                "sarcasm": SarcasmDetector(self.models.get("sarcasm", ""), dev),
                "sent": SentimentEngine(self.models.get("sentiment", ""), dev),
                "emo": EmotionEngine(self.models.get("emotion", ""), dev),
            }
        return self._ensemble

    # ------------------------------------------------------------- engines

    def _run_ensemble(self, texts):
        ens = self._get_ensemble()
        lids = ens["lid"].predict(texts)
        sarc = ens["sarcasm"].predict(texts)
        sents = ens["sent"].predict(texts)
        emos = ens["emo"].predict(texts)
        out = []
        for (lang, _), (is_s, s_conf), (s_lab, s_conf), (e_dom, e_conf, _d) \
                in zip(lids, sarc, sents, emos):
            irony = 1 if (is_s and s_conf >= self.irony_thr and s_lab == "positive") else 0
            out.append({"language_label": lang, "is_sarcastic": is_s,
                        "sarcasm_conf": s_conf, "irony_flag": irony,
                        "sentiment_label": s_lab, "sentiment_conf": s_conf,
                        "dominant_emotion": e_dom, "emotion_conf": e_conf,
                        "engine_used": "layer1_transformer"})
        return out

    def _run_lexicon(self, texts):
        out = []
        for t in texts:
            lang, _ = detect_language(t)
            r = self._lex.score(t)
            emo, dom, conf = r["emotions"], r["dominant"], r["confidence"]
            is_s = 1 if dom == "sarcasm" else 0
            s_conf = round(emo.get("sarcasm", 0) / 100, 3)
            if is_s:  # sarcasm becomes the flag; emotion falls to runner-up
                rest = {k: v for k, v in emo.items() if k != "sarcasm"}
                dom = max(rest, key=rest.get) if rest else "neutral"
                conf = rest.get(dom, 0)
            s_lab = ("negative" if dom in ("hostile", "fear")
                     else "positive" if dom in ("supportive", "excitement")
                     else "neutral")
            out.append({"language_label": lang, "is_sarcastic": is_s,
                        "sarcasm_conf": s_conf if is_s else round(s_conf, 3),
                        "irony_flag": 0, "sentiment_label": s_lab,
                        "sentiment_conf": conf, "dominant_emotion": dom,
                        "emotion_conf": conf, "engine_used": "layer2_lexicon"})
        return out

    # ------------------------------------------------------------ the cycle

    def analyze_cycle(self) -> dict:
        conn = processed_connect(self.proc_db)
        try:
            conn.execute(f"ATTACH DATABASE 'file:{self.vault_db}?mode=ro' AS vault")
            depth = conn.execute(
                "SELECT COUNT(*) FROM vault.messages m LEFT JOIN processed_messages p"
                " ON p.raw_message_id=m.id WHERE p.id IS NULL").fetchone()[0]
            rows = conn.execute(
                "SELECT m.id, m.source, m.author_id, m.posted_at_us, m.text"
                " FROM vault.messages m LEFT JOIN processed_messages p"
                " ON p.raw_message_id=m.id WHERE p.id IS NULL"
                " ORDER BY m.id LIMIT ?", (self.pull,)).fetchall()
            if not rows:
                return {"analyzed": 0, "depth": 0}

            use_gpu = (self._gpu_available() and depth < self.cap
                       and time.monotonic() >= self._lex_until)
            if not use_gpu and not self._lex_reason:
                self._lex_reason = ("forced" if self.force2 else
                                    "no CUDA" if not self._gpu_available() else
                                    f"backlog {depth}≥{self.cap}" if depth >= self.cap
                                    else "cooldown")

            records, n1, n2 = [], 0, 0
            for i in range(0, len(rows), self.batch):
                chunk = rows[i:i + self.batch]
                texts = [clean_text(r[4]) for r in chunk]
                if use_gpu:
                    t0 = time.monotonic()
                    try:
                        results = self._run_ensemble(texts)
                        dt_ms = (time.monotonic() - t0) * 1000
                        if dt_ms > self.timeout_ms:
                            self._lex_until = time.monotonic() + 60
                            self._lex_reason = f"slow batch {dt_ms:.0f}ms"
                            log.warning("hybrid: batch %.0fms > %dms — Layer 2 for 60s",
                                        dt_ms, self.timeout_ms)
                    except (RuntimeError, MemoryError) as e:   # CUDA OOM lands here
                        self._lex_until = time.monotonic() + self.cooldown_s
                        self._lex_reason = f"GPU fault: {e}"
                        log.warning("hybrid: GPU fault — Layer 2 cooldown %ss (%s)",
                                    self.cooldown_s, e)
                        results = self._run_lexicon(texts)
                else:
                    results = self._run_lexicon(texts)
                for (rid, source, author, posted, _raw), res, txt in zip(chunk, results, texts):
                    n1 += res["engine_used"].startswith("layer1")
                    n2 += res["engine_used"].startswith("layer2")
                    records.append((rid, source, author or "", posted, txt,
                                    tokens_json(filtered_tokens(txt)),
                                    res["engine_used"], res["language_label"],
                                    res["is_sarcastic"], res["sarcasm_conf"],
                                    res["irony_flag"], res["sentiment_label"],
                                    res["sentiment_conf"], res["dominant_emotion"],
                                    res["emotion_conf"], now_us()))

            conn.execute("BEGIN IMMEDIATE")
            conn.executemany(
                "INSERT OR IGNORE INTO processed_messages (raw_message_id, source,"
                " author_id, posted_at_us, clean_text, filtered_tokens, engine_used,"
                " language_label, is_sarcastic, sarcasm_conf, irony_flag,"
                " sentiment_label, sentiment_conf, dominant_emotion, emotion_conf,"
                " processed_at_us) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", records)
            for k, v in (("last_cycle_at", str(now_us())), ("last_depth", str(depth)),
                         ("last_layer1", str(n1)), ("last_layer2", str(n2)),
                         ("routing_reason", self._lex_reason or "gpu")):
                conn.execute("INSERT INTO hybrid_state (key, value) VALUES (?,?)"
                             " ON CONFLICT(key) DO UPDATE SET value=excluded.value", (k, v))
            conn.execute("COMMIT")
            if self._lex_until and time.monotonic() >= self._lex_until:
                self._lex_reason = ""          # cooldown expired
            return {"analyzed": len(records), "layer1": n1, "layer2": n2, "depth": depth}
        finally:
            try:
                conn.execute("DETACH DATABASE vault")
            except Exception:
                pass
            conn.close()


async def run_controller_loop(cfg, shutdown):
    c = HybridController(cfg)
    if c._gpu_available():
        log.info("hybrid pipeline up — Layer 1 (GPU ensemble) primary")
        c._get_ensemble()                     # warm the models at boot
    else:
        log.warning("hybrid pipeline up — CUDA unavailable, running Layer 2 (lexicon)"
                    " — install the CUDA torch build to enable the ensemble")
    while not shutdown.is_set():
        try:
            r = await asyncio.to_thread(c.analyze_cycle)
            if r["analyzed"]:
                log.info("hybrid: analyzed %d (L1=%d L2=%d, backlog=%d)",
                         r["analyzed"], r["layer1"], r["layer2"], r["depth"])
                await asyncio.sleep(0.2)
            else:
                await asyncio.sleep(2)
        except Exception:
            log.exception("hybrid cycle failed")
            await asyncio.sleep(5)