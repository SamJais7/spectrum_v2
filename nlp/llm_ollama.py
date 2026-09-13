"""Local LLM via Ollama (blueprint §7). Stdlib HTTP only — no new dependency.
Every call degrades gracefully when the daemon is down."""

import json
import logging
import re
import urllib.request

log = logging.getLogger("collector.llm")


class OllamaClient:
    def __init__(self, cfg: dict):
        cfg = cfg or {}
        self.base = cfg.get("base_url", "http://127.0.0.1:11434").rstrip("/")
        self.model = cfg.get("model", "qwen2.5:3b-instruct")
        self.timeout = int(cfg.get("timeout_s", 90))
        self.keep_alive = cfg.get("keep_alive", 0)

    def _post(self, path, payload, timeout=None):
        req = urllib.request.Request(self.base + path,
                                     data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout or self.timeout) as r:
            return json.loads(r.read().decode())

    def available(self) -> bool:
        try:
            with urllib.request.urlopen(self.base + "/api/tags", timeout=3) as r:
                return r.status == 200
        except Exception:
            return False

    def generate(self, prompt: str, system: str = "", max_len: int = 400) -> str:
        payload = {"model": self.model, "prompt": prompt, "stream": False,
                   "keep_alive": self.keep_alive,
                   "options": {"temperature": 0.2, "num_predict": max_len}}
        if system:
            payload["system"] = system
        return (self._post("/api/generate", payload).get("response") or "").strip()

    def title_cluster(self, keywords, exemplars) -> str:
        try:
            p = ("Keywords: " + ", ".join(keywords) + "\nExample messages:\n"
                 + "\n".join(f"- {e[:160]}" for e in exemplars)
                 + "\n\nGive a concise 4-7 word title for the shared topic."
                   " Output ONLY the title, no quotes.")
            t = re.sub(r'["\n]', " ", self.generate(p, max_len=40)).strip()
            if 2 <= len(t.split()) <= 9:
                return t
        except Exception as e:
            log.debug("ollama titling failed: %s", e)
        return " · ".join(keywords[:3])               # deterministic fallback

    def brief(self, question, hits) -> str:
        lines = "\n".join(
            f"[{i+1}] ({h.get('source')}, {h.get('sentiment')}) "
            f"{(h.get('clean_text') or '')[:200]}"
            for i, h in enumerate(hits[:20]))
        try:
            return self.generate(
                f"Question: {question}\n\nRetrieved posts:\n{lines}\n\n"
                "Write exactly three bullet points answering the question using only"
                " these posts. Cite as [n].",
                system="You are a precise analyst assistant. Never invent facts.")
        except Exception as e:
            return f"(local LLM unavailable: {e}) — raw hits below are the evidence."