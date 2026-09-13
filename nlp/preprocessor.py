"""Two-tier text preprocessing (blueprint §2):
  clean_text()       — canonical semantic cleaning for transformers/embeddings/LLMs
  filtered_tokens()  — lowercase, punctuation/digit-stripped, custom-stopword-pruned,
                       lemmatized keyword arrays for c-TF-IDF (Phase 5)
NLTK lemmatization degrades gracefully to identity if unavailable."""

import json
import re

_URL_RE = re.compile(r"https?://\S+|www\.\S+|t\.me/\S+", re.IGNORECASE)
_MENTION_RE = re.compile(r"@\w+")
# Astral-plane strip (blueprint): removes ALL emoji — and all CJK. Acceptable for
# an EN/Hinglish pipeline; revisit if CJK targets are ever added.
_EMOJI_RE = re.compile(r"[\U00010000-\U0010ffff\u2600-\u27bf\ufe0f\u200d]")
_ESCAPE_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\u200b-\u200f\u2060\ufeff]")
_WS_RE = re.compile(r"[ \t]+")
_TOKEN_RE = re.compile(r"[a-z]+")

_STOP_BASE = set("""a an and are as at be been but by can cant could did do does dont for from
had has have he her here hers him his how i if in into is it its just like me more most my
no not now of on or our out over own re rt she should so some such than that the their them
then there these they this those to too up us via was we were what when where which who why
will with wont you your youre theyre thats its im ive get got one two people time new today
say says said also about after all am any because before being between both each even first
great here keep last let made make many much never next off old only other real right same
still take tell thing think want way well went work year years day days going good know look
make see thing things want watch week what world would yeah yes yet back come end need say
use used using want way made man men mean much might since start stop sure take turn two""".split())

_PLATFORM_NOISE = {"rt", "dm", "join", "link", "channel", "admin", "group",
                   "pinned", "bot", "free"}
_HINGLISH_FILLER = {"bhai", "yaar", "hai", "karo", "nahi", "kya", "yeh", "woh",
                    "hain", "kya", "tha", "hoga", "kar", "mera", "tera", "aur"}
STOPWORDS = _STOP_BASE | _PLATFORM_NOISE | _HINGLISH_FILLER

_LEMMA = None  # None=uninit, False=unavailable, obj=ready


def _lemmatizer():
    global _LEMMA
    if _LEMMA is None:
        try:
            from nltk.stem import WordNetLemmatizer
            import nltk
            try:
                nltk.data.find("corpora/wordnet")
            except LookupError:
                nltk.download("wordnet", quiet=True)
            _LEMMA = WordNetLemmatizer()
        except Exception:
            _LEMMA = False
    return _LEMMA


def clean_text(text: str) -> str:
    t = _URL_RE.sub(" ", text or "")
    t = _MENTION_RE.sub(" ", t)
    t = _EMOJI_RE.sub(" ", t)
    t = _ESCAPE_RE.sub(" ", t)
    t = _WS_RE.sub(" ", t)
    t = re.sub(r"\n{2,}", "\n", t)
    return t.strip()


def filtered_tokens(text: str) -> list:
    toks = _TOKEN_RE.findall((text or "").lower())
    lem = _lemmatizer()
    if lem:
        toks = [lem.lemmatize(t) for t in toks]
    return [t for t in toks if t not in STOPWORDS and len(t) > 2]


def tokens_json(tokens: list) -> str:
    return json.dumps(tokens, ensure_ascii=False)