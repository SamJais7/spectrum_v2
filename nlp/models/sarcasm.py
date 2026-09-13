import logging

log = logging.getLogger("collector.nlp.sarcasm")
_SARC_LABELS = {"sarcastic", "label_1", "1", "irony", "ironic"}


class SarcasmDetector:
    def __init__(self, model_id: str, device=-1):
        self._pipe = None
        try:
            from transformers import pipeline
            self._pipe = pipeline("text-classification", model=model_id, device=device)
            log.info("sarcasm detector loaded: %s", model_id)
        except Exception as e:
            log.warning("sarcasm model %r unavailable (%s) — sarcasm=0", model_id, e)

    def predict(self, texts):
        """Returns [(is_sarcastic 0/1, p_sarcastic)]."""
        if self._pipe is None:
            return [(0, 0.0) for _ in texts]
        out = self._pipe(list(texts))
        res = []
        for r in out:
            lab = str(r["label"]).lower()
            score = float(r["score"])
            if lab in _SARC_LABELS:
                res.append((1, round(score, 3)))
            else:                       # top class was non-sarcastic
                res.append((0, round(1.0 - score, 3)))
        return res