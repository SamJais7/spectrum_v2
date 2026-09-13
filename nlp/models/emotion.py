import logging

log = logging.getLogger("collector.nlp.emotion")

# GoEmotions 28 -> business taxonomy. Blueprint's table covered 19 labels;
# the remaining 9 are assigned here (see code comments):
GO_TAXONOMY = {
    "admiration": "supportive", "approval": "supportive", "gratitude": "supportive",
    "love": "supportive", "caring": "supportive", "relief": "supportive",
    "anger": "hostile", "annoyance": "hostile", "disgust": "hostile",
    "disapproval": "hostile",
    "fear": "fear", "nervousness": "fear",
    # taxonomy has no sadness bucket; distress family -> fear (documented choice):
    "sadness": "fear", "grief": "fear", "disappointment": "fear",
    "embarrassment": "fear", "remorse": "fear",
    "excitement": "excitement", "joy": "excitement", "optimism": "excitement",
    "pride": "excitement", "amusement": "excitement", "desire": "excitement",
    "confusion": "confusion", "curiosity": "confusion", "surprise": "confusion",
    "neutral": "neutral", "realization": "neutral",
}


class EmotionEngine:
    def __init__(self, model_id: str, device=-1):
        self._pipe = None
        try:
            from transformers import pipeline
            self._pipe = pipeline("text-classification", model=model_id,
                                  top_k=None, device=device)
            log.info("emotion engine loaded: %s", model_id)
        except Exception as e:
            log.warning("emotion model %r unavailable (%s)", model_id, e)

    def predict(self, texts):
        """Returns [(dominant, conf, full_distribution)]."""
        if self._pipe is None:
            return [("neutral", 1.0, {"neutral": 1.0}) for _ in texts]
        out = self._pipe(list(texts))
        res = []
        for doc in out:
            scores = {}
            for r in doc:
                ours = GO_TAXONOMY.get(str(r["label"]).lower())
                if ours:
                    scores[ours] = scores.get(ours, 0.0) + float(r["score"])
            if not scores:
                scores = {"neutral": 1.0}
            dom = max(scores, key=scores.get)
            res.append((dom, round(scores[dom], 3), {k: round(v, 3) for k, v in scores.items()}))
        return res