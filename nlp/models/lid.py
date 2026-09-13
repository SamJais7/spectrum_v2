import logging

log = logging.getLogger("collector.nlp.lid")


class LanguageIdentifier:
    """Hinglish/code-mixed LID. The blueprint's model ID (l3cube-pune/hing-bert-lid)
    is UNVERIFIED — if it fails to load, we degrade to the builtin detector and
    say so in the log. Verify the ID on huggingface.co and correct config."""

    def __init__(self, model_id: str, device=-1):
        self._pipe = None
        try:
            from transformers import pipeline
            self._pipe = pipeline("text-classification", model=model_id, device=device)
            log.info("LID loaded: %s", model_id)
        except Exception as e:
            log.warning("LID model %r unavailable (%s) — using builtin detector",
                        model_id, e)

    def predict(self, texts):
        from nlp.engines import detect_language
        if self._pipe is None:
            return [detect_language(t) for t in texts]
        out = self._pipe(list(texts), top_k=1)
        res = []
        for o in out:
            r = o[0] if isinstance(o, list) else o
            lab = str(r["label"]).lower()
            if "hing" in lab:
                lab = "hinglish"
            elif lab in ("label_1", "1"):
                lab = "hinglish"
            else:
                lab = "en" if ("en" in lab or lab in ("label_0", "0")) else lab
            res.append((lab, round(float(r["score"]), 3)))
        return res