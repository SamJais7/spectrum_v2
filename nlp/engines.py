"""Three analysers, each pure-Python and dependency-free by default:

* detect_language     — script ranges + Latin stopword scoring (short-text friendly)
* EmotionEngine       — multi-label (supportive/hostile/sarcasm/fear/excitement + neutral),
                        confidence 0-100 per label. LexiconEngine: transparent, fast,
                        offline. MLEngine: zero-shot transformer (set engine: ml).
* infer_demographics  — age bucket / location / language / professional interest from
                        PUBLIC bio + declared location only. Aggregate buckets with
                        confidences — never identity, never personal data.
"""

import math
import re
from datetime import datetime, timezone

EMOTION_LABELS = ("supportive", "hostile", "sarcasm", "fear", "excitement", "neutral")

# --------------------------------------------------------------- language

_SCRIPTS = [("ja", 0x3040, 0x30FF), ("ko", 0xAC00, 0xD7AF), ("zh", 0x4E00, 0x9FFF),
            ("ru", 0x0400, 0x04FF), ("el", 0x0370, 0x03FF), ("he", 0x0590, 0x05FF),
            ("ar", 0x0600, 0x06FF), ("hi", 0x0900, 0x097F), ("th", 0x0E00, 0x0E7F)]
_LATIN_STOP = {
    "en": {"the", "and", "is", "are", "was", "you", "for", "that", "this", "with", "have",
           "not", "but", "they", "what", "just", "about", "can", "will"},
    "es": {"que", "de", "la", "el", "los", "las", "y", "en", "un", "una", "por", "con",
           "para", "pero", "muy", "esto", "son"},
    "pt": {"que", "de", "não", "uma", "com", "para", "mas", "muito", "você", "são"},
    "fr": {"le", "la", "les", "des", "que", "et", "est", "dans", "un", "une", "pour",
           "avec", "pas", "mais", "nous", "vous"},
    "de": {"der", "die", "das", "und", "ist", "nicht", "mit", "für", "auf", "den",
           "dem", "ein", "eine", "auch", "wir", "sie"},
    "it": {"che", "di", "per", "con", "non", "sono", "anche", "come", "questo", "gli", "nel"},
    "nl": {"de", "het", "een", "en", "van", "is", "dat", "niet", "met", "voor", "zijn"},
    "tr": {"bir", "ve", "bu", "için", "ile", "değil", "çok", "ama", "gibi", "daha", "var"},
    "id": {"yang", "dan", "di", "itu", "dengan", "untuk", "tidak", "ini", "dari", "dalam"},
    "pl": {"nie", "jest", "się", "na", "że", "do", "jak", "ale", "tak", "przez", "tylko"},
}
_CHAR_HINTS = {"es": "ñ", "pt": "ãõç", "fr": "éèêçàù", "de": "äöüß",
               "pl": "ąćęłńóśźż", "tr": "ışğ", "it": "àùò"}
_LANG_REGION = {"ru": "Russia/CIS", "uk": "Ukraine", "ar": "MENA", "zh": "China",
                "ja": "Japan", "ko": "Korea", "es": "Spain/LatAm",
                "pt": "Brazil/Portugal", "fr": "France", "de": "DACH", "tr": "Türkiye",
                "hi": "India", "th": "Thailand", "id": "Indonesia", "it": "Italy",
                "nl": "Netherlands", "pl": "Poland", "el": "Greece", "vi": "Vietnam"}


def detect_language(text: str):
    """Returns (code, confidence 0-1)."""
    if not text:
        return "en", 0.2
    counts = {}
    for ch in text:
        for code, lo, hi in _SCRIPTS:
            if lo <= ord(ch) <= hi:
                counts[code] = counts.get(code, 0) + 1
                break
    best_script = max(counts, key=counts.get) if counts else None
    if best_script and counts[best_script] >= 2:
        return best_script, 0.95
    toks = re.findall(r"[a-zà-ÿäöüßąćęłńóśźżışğ']+", text.lower())
    if not toks:
        return "en", 0.2
    scores = {}
    for code, stops in _LATIN_STOP.items():
        s = 3 * sum(1 for t in toks if t in stops)
        s += sum(text.lower().count(c) for c in _CHAR_HINTS.get(code, ""))
        if s:
            scores[code] = s
    if not scores:
        return "en", 0.3
    code, s = max(scores.items(), key=lambda kv: kv[1])
    conf = min(0.95, 0.4 + 0.08 * s)
    return code, round(conf, 2)


# --------------------------------------------------------------- emotion

_PHRASES = {
    "supportive": [("thanks", 1.6), ("thank you", 2.0), ("appreciate", 1.6),
        ("great job", 2.2), ("well done", 2.2), ("well said", 2.0), ("love this", 2.0),
        ("love it", 1.8), ("congrats", 2.0), ("congratulations", 2.2), ("proud of", 1.8),
        ("i support", 2.0), ("standing with", 2.0), ("i agree", 1.8), ("agree", 1.2),
        ("spot on", 2.0), ("nailed it", 2.0), ("couldn't agree more", 2.4),
        ("brilliant", 1.6), ("respect", 1.4), ("hats off", 2.0), ("rooting for", 1.8),
        ("glad", 1.2), ("helpful", 1.4), ("insightful", 1.6), ("impressive", 1.6),
        ("awesome", 1.4), ("amazing", 1.4), ("you got this", 1.8), ("keep going", 1.6)],
    "hostile": [("hate", 2.0), ("idiot", 2.2), ("idiots", 2.2), ("stupid", 1.8),
        ("moron", 2.2), ("morons", 2.2), ("dumb", 1.6), ("clown", 1.8), ("clowns", 1.8),
        ("loser", 2.0), ("trash", 1.6), ("garbage", 1.6), ("pathetic", 2.0),
        ("scum", 2.2), ("scumbag", 2.4), ("disgusting", 1.8), ("appalling", 1.8),
        ("shameful", 1.6), ("shame on", 2.0), ("terrible", 1.4), ("awful", 1.4),
        ("horrible", 1.4), ("liar", 2.0), ("liars", 2.0), ("fraud", 1.8), ("scam", 1.6),
        ("corrupt", 1.6), ("traitor", 2.0), ("delusional", 1.8), ("clown show", 2.0),
        ("shut up", 2.0), ("stfu", 2.2), ("gtfo", 1.8), ("what a joke", 1.8),
        ("absolute joke", 2.0), ("screw you", 1.8), ("vile", 1.8), ("shill", 1.8),
        ("bootlicker", 1.8), ("braindead", 2.0), ("worst", 1.2)],
    "sarcasm": [("#sarcasm", 3.0), ("#sarcastic", 3.0), ("yeah right", 2.4),
        ("oh great", 2.2), ("oh wonderful", 2.4), ("oh joy", 2.2), ("thanks a lot", 2.4),
        ("thanks so much for that", 2.6), ("sure thing", 1.6), ("totally", 0.8),
        ("obviously", 0.8), ("because that worked so well", 3.0),
        ("because that went well", 3.0), ("love how", 2.0), ("i love how", 2.2),
        ("how shocking", 2.4), ("what a surprise", 2.4), ("big surprise", 2.2),
        ("who would have thought", 2.6), ("who could have guessed", 2.6),
        ("groundbreaking", 2.0), ("revolutionary", 1.6), ("genius", 1.8),
        ("absolute genius", 2.4), ("nice one", 1.6), ("well played", 1.8),
        ("good luck with that", 2.2), ("said no one ever", 3.0),
        ("love that for you", 2.4), ("as expected", 1.4), ("makes total sense", 1.8),
        ("nothing says", 1.8), ("living rent free", 1.2)],
    "fear": [("scared", 2.0), ("terrified", 2.4), ("afraid", 1.8), ("frightened", 2.0),
        ("worried", 1.8), ("worrying", 1.6), ("anxious", 2.0), ("anxiety", 1.8),
        ("panic", 1.8), ("panicking", 2.0), ("dread", 1.8), ("dreading", 2.0),
        ("uneasy", 1.6), ("nervous", 1.6), ("freaked out", 2.0), ("nightmare", 1.6),
        ("horrified", 2.2), ("alarmed", 1.8), ("danger", 1.4), ("dangerous", 1.4),
        ("doom", 1.6), ("pray for", 1.8), ("stay safe", 1.4), ("keep us safe", 1.8),
        ("scary", 1.8), ("creepy", 1.4), ("can't sleep", 1.6),
        ("freaking me out", 2.0), ("what if", 1.2), ("crisis", 1.0),
        ("collapse", 1.2), ("worse than we thought", 2.0)],
    "excitement": [("excited", 2.2), ("exciting", 2.0), ("can't wait", 2.0),
        ("hyped", 2.0), ("so hyped", 2.4), ("pumped", 1.8), ("let's go", 1.8),
        ("lets go", 1.6), ("lfg", 2.0), ("to the moon", 2.4), ("bullish", 1.8),
        ("breakthrough", 1.8), ("finally", 1.2), ("it's happening", 2.2),
        ("huge news", 2.0), ("massive", 1.2), ("wow", 1.6), ("omg", 1.6),
        ("stoked", 2.0), ("thrilled", 2.2), ("epic", 1.6), ("legendary", 1.6),
        ("historic", 1.6), ("huge if true", 2.2), ("game changer", 2.0),
        ("mind blowing", 2.0), ("we're so back", 2.2), ("yesss", 1.8), ("wooo", 1.6)],
}
_EMOJI = {"😂": ("sarcasm", 1.0), "🤣": ("sarcasm", 1.2), "🙄": ("sarcasm", 2.0),
          "💀": ("sarcasm", 0.6), "😱": ("fear", 2.0), "😨": ("fear", 2.0),
          "😰": ("fear", 1.6), "😭": ("fear", 0.8), "😡": ("hostile", 2.0),
          "🤬": ("hostile", 2.4), "👿": ("hostile", 2.0), "❤️": ("supportive", 1.5),
          "😍": ("supportive", 1.5), "🥰": ("supportive", 1.5), "👍": ("supportive", 1.2),
          "🙌": ("supportive", 1.2), "💪": ("supportive", 1.0), "🚀": ("excitement", 2.0),
          "🌕": ("excitement", 1.6), "🔥": ("excitement", 1.5),
          "🎉": ("excitement", 1.5), "✨": ("excitement", 1.0)}
_NEGATIONS = {"not", "never", "no", "isn't", "wasn't", "don't", "doesn't",
              "can't", "won't", "ain't"}
_INTENSIFIERS = {"so", "very", "really", "absolutely", "extremely", "super",
                 "insanely", "fucking", "fkn"}
_FLIP = {"supportive": "hostile", "hostile": "supportive"}
_TOK = re.compile(r"[a-z0-9']+|#[a-z0-9_]+")
_PHRASE_MAP = {}
for _e, _lst in _PHRASES.items():
    for _p, _w in _lst:
        _PHRASE_MAP.setdefault(tuple(_p.split()), []).append((_e, _w))


class LexiconEmotionEngine:
    name = "lexicon"

    def score(self, text: str) -> dict:
        low = text.lower()
        toks = _TOK.findall(low)
        s = {e: 0.0 for e in EMOTION_LABELS if e != "neutral"}
        for i in range(len(toks)):
            for n in (4, 3, 2, 1):                      # longest match wins at position
                if i + n > len(toks):
                    continue
                hits = _PHRASE_MAP.get(tuple(toks[i:i + n]))
                if hits:
                    back = toks[max(0, i - 3):i]
                    mult = 1.5 if any(b in _INTENSIFIERS for b in back) else 1.0
                    neg = any(b in _NEGATIONS for b in back)
                    for emo, w in hits:                 # "not great" flips polarity
                        if neg and emo in _FLIP:
                            s[_FLIP[emo]] += w * 0.8 * mult
                        else:
                            s[emo] += w * mult
                    break
        for ch, (emo, w) in _EMOJI.items():
            c = text.count(ch)
            if c:
                s[emo] += w * c
        if re.search(r"(^|\s)/s(\s|$)", low):
            s["sarcasm"] += 2.5
        caps = min(len(re.findall(r"\b[A-Z]{4,}\b", text)), 3)
        if caps:
            s["excitement"] += 0.4 * caps
            s["hostile"] += 0.3 * caps
        if text.count("!") >= 2:
            s["excitement"] += 0.3
        if s["sarcasm"] >= 1.0:                          # irony steals positive words
            s["supportive"] *= 0.5
        neutral = 1.1 + (0.5 if max(s.values()) < 0.8 else 0.0)
        items = dict(s, neutral=neutral)
        exps = {k: math.exp(1.3 * v) for k, v in items.items()}
        tot = sum(exps.values())
        pct = {k: round(100 * v / tot) for k, v in exps.items()}
        dom = max(pct, key=pct.get)
        return {"emotions": pct, "dominant": dom, "confidence": pct[dom]}


class MLEmotionEngine:
    """Zero-shot classification with our custom label set. Higher fidelity,
    ~50-200 posts/s on CPU, far more on GPU. Returns independent confidences."""
    name = "ml"
    _LABELS = ["a supportive, encouraging or grateful statement",
               "a hostile, attacking or insulting statement",
               "a sarcastic or ironic statement",
               "a fearful, anxious or worried statement",
               "an excited, thrilled or celebratory statement"]
    _KEYS = ["supportive", "hostile", "sarcasm", "fear", "excitement"]

    def __init__(self):
        self._pipe = None

    def _load(self):
        from transformers import pipeline            # optional dependency
        self._pipe = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

    def score(self, text: str) -> dict:
        if self._pipe is None:
            self._load()
        out = self._pipe(text[:1000], self._LABELS, multi_label=True)
        conf = {k: round(100 * s) for k, s in zip(self._KEYS, out["scores"])}
        dom = max(conf, key=conf.get)
        if conf[dom] < 50:
            conf["neutral"] = max(100 - conf[dom], 0)
            dom = "neutral"
        return {"emotions": conf, "dominant": dom, "confidence": conf[dom]}


def make_engine(ncfg: dict):
    return MLEmotionEngine() if ncfg.get("engine") == "ml" else LexiconEmotionEngine()


# --------------------------------------------------------------- demographics

_AGE_BUCKETS = [(18, "under_18"), (26, "18-25"), (36, "26-35"), (51, "35-50"), (999, "50+")]


def _bucket(age: int) -> str:
    for hi, name in _AGE_BUCKETS:
        if age < hi:
            return name
    return "50+"


def _age_from_bio(bio: str):
    """Returns (bucket, confidence, evidence) or None."""
    low = (bio or "").lower()
    year = datetime.now(timezone.utc).year
    m = re.search(r"\bborn (?:in )?((?:19|20)\d{2})\b", low)
    if m:
        return _bucket(year - int(m.group(1))), 0.75, f"born {m.group(1)}"
    m = re.search(r"\b(\d{2})\s*(?:yo|y/o|years? old|yrs? old)\b", low)
    if m:
        return _bucket(int(m.group(1))), 0.8, f"states age {m.group(1)}"
    m = re.search(r"\bclass of (20\d{2})\b", low)
    if m:
        grad = int(m.group(1))
        return _bucket(18 - (grad - year) if grad > year else 18), 0.6, f"class of {grad}"
    marks = [
        (r"\bhigh school\b|\bdropout\b|\bteen(?:ager)?\b", "under_18", 2.0),
        (r"\bcollege student\b|\bundergrad\b|\buniversity student\b|\bfreshman\b|"
         r"\bsophomore\b|\bgen z\b|\bzoomer\b|\bfrat\b|\bsorority\b", "18-25", 2.0),
        (r"\bstudent\b|\buni\b", "18-25", 0.8),
        (r"\bphd candidate\b|\bgrad student\b|\bmsc student\b", "26-35", 1.5),
        (r"\bfounder\b|\bentrepreneur\b|\bengineer\b|\banalyst\b|\bdeveloper\b|"
         r"\bmanager\b|\bstartup\b", "26-35", 1.0),
        (r"\bsenior\b|\bdirector of\b|\bhead of\b|\bvp of\b|\bceo\b|\bcfo\b|\bcto\b|"
         r"\bcoo\b|\bpartner\b|\bconsultant\b", "35-50", 1.5),
        (r"\b(?:mom|dad|mother|father|wife|husband|parent) of\b", "35-50", 1.2),
        (r"\bretired\b|\bgrand(?:pa|ma|father|mother)\b|\bveteran\b|\bboomer\b|"
         r"\b\d{2}\+? years (?:of|in)\b", "50+", 2.0),
    ]
    hits = [(b, w) for rx, b, w in marks if re.search(rx, low)]
    if hits:
        best = max(hits, key=lambda x: x[1])[0]
        conf = min(0.7, 0.2 + 0.15 * sum(w for _, w in hits))
        return best, round(conf, 2), [h for h, _ in hits]
    return None


_LOC = {
    "usa": "United States", "us": "United States", "united states": "United States",
    "america": "United States", "nyc": "United States", "new york": "United States",
    "california": "United States", "texas": "United States", "florida": "United States",
    "los angeles": "United States", "chicago": "United States", "boston": "United States",
    "seattle": "United States", "san francisco": "United States", "washington dc": "United States",
    "colorado": "United States", "michigan": "United States", "arizona": "United States",
    "nevada": "United States", "oregon": "United States", "virginia": "United States",
    "new jersey": "United States", "pennsylvania": "United States",
    "massachusetts": "United States", "georgia": "United States", "ohio": "United States",
    "uk": "United Kingdom", "united kingdom": "United Kingdom", "britain": "United Kingdom",
    "england": "United Kingdom", "london": "United Kingdom", "scotland": "United Kingdom",
    "wales": "United Kingdom", "manchester": "United Kingdom", "birmingham": "United Kingdom",
    "liverpool": "United Kingdom", "edinburgh": "United Kingdom",
    "canada": "Canada", "toronto": "Canada", "vancouver": "Canada", "montreal": "Canada",
    "ottawa": "Canada", "calgary": "Canada",
    "australia": "Australia", "sydney": "Australia", "melbourne": "Australia",
    "brisbane": "Australia", "perth": "Australia", "aussie": "Australia",
    "new zealand": "New Zealand", "auckland": "New Zealand",
    "germany": "Germany", "berlin": "Germany", "munich": "Germany", "hamburg": "Germany",
    "frankfurt": "Germany", "deutschland": "Germany",
    "france": "France", "paris": "France", "lyon": "France", "marseille": "France",
    "spain": "Spain", "madrid": "Spain", "barcelona": "Spain", "españa": "Spain",
    "italy": "Italy", "rome": "Italy", "milan": "Italy", "italia": "Italy", "naples": "Italy",
    "netherlands": "Netherlands", "amsterdam": "Netherlands", "holland": "Netherlands",
    "russia": "Russia", "moscow": "Russia", "siberia": "Russia", "st petersburg": "Russia",
    "ukraine": "Ukraine", "kyiv": "Ukraine", "kiev": "Ukraine", "lviv": "Ukraine",
    "poland": "Poland", "warsaw": "Poland", "krakow": "Poland",
    "turkey": "Türkiye", "turkiye": "Türkiye", "istanbul": "Türkiye", "ankara": "Türkiye",
    "brazil": "Brazil", "brasil": "Brazil", "sao paulo": "Brazil", "são paulo": "Brazil",
    "rio de janeiro": "Brazil", "rio": "Brazil",
    "mexico": "Mexico", "mexico city": "Mexico", "monterrey": "Mexico",
    "argentina": "Argentina", "buenos aires": "Argentina",
    "india": "India", "mumbai": "India", "delhi": "India", "bangalore": "India",
    "bengaluru": "India", "chennai": "India", "hyderabad": "India", "kolkata": "India",
    "china": "China", "beijing": "China", "shanghai": "China", "shenzhen": "China",
    "japan": "Japan", "tokyo": "Japan", "osaka": "Japan", "kyoto": "Japan",
    "south korea": "South Korea", "korea": "South Korea", "seoul": "South Korea",
    "singapore": "Singapore", "hong kong": "Hong Kong", "taiwan": "Taiwan", "taipei": "Taiwan",
    "dubai": "UAE", "abu dhabi": "UAE", "uae": "UAE", "united arab emirates": "UAE",
    "israel": "Israel", "tel aviv": "Israel", "jerusalem": "Israel",
    "saudi arabia": "Saudi Arabia", "riyadh": "Saudi Arabia", "jeddah": "Saudi Arabia",
    "nigeria": "Nigeria", "lagos": "Nigeria", "kenya": "Kenya", "nairobi": "Kenya",
    "south africa": "South Africa", "johannesburg": "South Africa", "cape town": "South Africa",
    "egypt": "Egypt", "cairo": "Egypt", "iran": "Iran", "tehran": "Iran",
    "pakistan": "Pakistan", "karachi": "Pakistan", "lahore": "Pakistan",
    "indonesia": "Indonesia", "jakarta": "Indonesia", "bali": "Indonesia",
    "vietnam": "Vietnam", "hanoi": "Vietnam", "thailand": "Thailand", "bangkok": "Thailand",
    "philippines": "Philippines", "manila": "Philippines",
    "greece": "Greece", "athens": "Greece", "portugal": "Portugal", "lisbon": "Portugal",
    "sweden": "Sweden", "stockholm": "Sweden", "norway": "Norway", "oslo": "Norway",
    "denmark": "Denmark", "copenhagen": "Denmark", "finland": "Finland", "helsinki": "Finland",
    "switzerland": "Switzerland", "zurich": "Switzerland", "geneva": "Switzerland",
    "austria": "Austria", "vienna": "Austria", "belgium": "Belgium", "brussels": "Belgium",
    "ireland": "Ireland", "dublin": "Ireland",
}
_LOC_SORTED = sorted(_LOC.items(), key=lambda kv: -len(kv[0]))

_INTERESTS = {
    "tech": ["software", "engineer", "developer", "programmer", "coding", " tech",
             "machine learning", " ai ", "data science", "devops", "sre", "cloud",
             "aws", "backend", "frontend", "full stack", "open source", "cto",
             "startup", "hacker", "linux", "python", "javascript", "typescript"],
    "crypto": ["crypto", "bitcoin", "btc", "ethereum", "defi", "web3", "nft", "blockchain",
               "solana", "altcoin", "hodl", "degen"],
    "finance": ["finance", "investor", "investing", "trader", "trading", "stocks",
                "markets", "hedge fund", "banking", "accountant", "cfa", "economist",
                "forex", "options", "venture capital"],
    "politics": ["politics", "political", "activist", "campaign", "election", "policy",
                 "geopolitics", "senator", "congress", "parliament", "conservative",
                 "liberal", "libertarian", "commentator"],
    "sports": ["football", "soccer", "nba", "nfl", "fifa", "hockey", "cricket", "tennis",
               "ufc", "boxing", "f1", "sports", "athlete"],
    "entertainment": ["music", "musician", "dj ", "producer", "actor", "film", "movie",
                      "gaming", "streamer", "twitch", "youtube", "podcast",
                      "content creator", "comedian"],
    "journalism": ["journalist", "reporter", "news", "editor", "correspondent",
                   "columnist", "writer", "author", "blogger", "media"],
    "health": ["doctor", "nurse", "medical", "medicine", "health", "clinician", "pharma",
               "psychologist", "therapist", "surgeon", "dentist"],
    "education": ["teacher", "professor", "lecturer", "phd", "researcher", "academic",
                  "educator", "tutor"],
    "business": ["marketing", "seo", "growth", "brand", "sales", "ecommerce",
                 "e-commerce", "founder", "ceo", "entrepreneur", "consultant",
                 "real estate", "recruiter"],
    "art": ["designer", "artist", "illustrator", "photographer", " ux ", "creative",
            "architect"],
}


def infer_demographics(profile, language: str) -> dict:
    """profile: row/dict with .bio/.location (or keys), may be None."""
    bio = (profile.get("bio") if isinstance(profile, dict) else getattr(profile, "bio", None)) or ""
    loc_field = (profile.get("location") if isinstance(profile, dict)
                 else getattr(profile, "location", None)) or ""
    ev = {}

    age = _age_from_bio(bio)
    if age:
        ev["age"] = age[2]

    location, loc_conf, loc_src = None, 0.0, None
    for text, conf, src in ((loc_field, 0.85, "declared"), (bio, 0.5, "bio")):
        if not text:
            continue
        low = " " + re.sub(r"[^\w]+", " ", text.lower()) + " "
        for alias, country in _LOC_SORTED:
            if f" {alias} " in low:
                location, loc_conf, loc_src = country, conf, src
                break
        if location:
            break
    if not location and language in _LANG_REGION:
        location, loc_conf, loc_src = _LANG_REGION[language], 0.3, "language"

    low_bio = " " + bio.lower() + " "
    hits = {k: sum(low_bio.count(w) for w in words) for k, words in _INTERESTS.items()}
    hits = {k: v for k, v in hits.items() if v}
    interest, int_conf = None, 0.0
    if hits:
        interest = max(hits, key=lambda k: hits[k])
        int_conf = min(0.8, 0.25 + 0.15 * hits[interest])
        ev["interest_hits"] = [k for k in hits]

    lang, lang_conf = detect_language(bio) if bio.strip() else (language, 0.4)

    return {"age_bucket": age[0] if age else None,
            "age_conf": age[1] if age else 0.0,
            "location": location, "location_conf": loc_conf,
            "language": lang, "language_conf": lang_conf,
            "interest": interest, "interest_conf": round(int_conf, 2),
            "evidence": ev, "location_source": loc_src}