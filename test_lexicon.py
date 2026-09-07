# test_lexicon.py
from nlp.engines import LexiconEmotionEngine

e = LexiconEmotionEngine()
tests = [
    "thanks so much, this is amazing!!",
    "what a joke, obviously that worked well 🙄",
    "I'm genuinely terrified about this",
    "just a normal weather report for tuesday",
]
for t in tests:
    r = e.score(t)
    print(f"{t!r}")
    print(f"   dominant: {r['dominant']} ({r['confidence']}%)\n   all: {r['emotions']}\n")