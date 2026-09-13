import logging

log = logging.getLogger("collector.nlp.sentiment")
_LABELMAP = {"label_0": "negative", "label_1": "neutral", "label_2": "positive"}


class SentimentEngine:
    def __init__(self, model_id: str, device=-1):
        self._pipe = None
        try:
            from transformers import pipeline
            self._pipe = pipeline("text-classification", model=model_id, device=device)
            log.info("sentiment engine loaded: %s", model_id)
        except Exception as e:
            log.warning("sentiment model %r unavailable (%s)", model_id, e)

    def predict(self, texts):
        """Returns [(label in pos/neg/neutral, conf)]."""
        if self._pipe is None:
            return [("neutral", 0.5) for _ in texts]
        out = self._pipe(list(texts))
        res = []
        for r in out:
            lab = _LABELMAP.get(str(r["label"]).lower(), str(r["label"]).lower())
            if lab not in ("positive", "negative", "neutral"):
                lab = "neutral"
            res.append((lab, round(float(r["score"]), 3)))
        return res