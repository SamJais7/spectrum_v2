from nlp.engines import MLEmotionEngine

e = MLEmotionEngine()
for t in ["thanks so much, this is amazing!!",
          "what a joke, obviously that worked well 🙄",
          "I'm genuinely terrified about this"]:
    print(f"{t!r}\n   -> {e.score(t)}\n")