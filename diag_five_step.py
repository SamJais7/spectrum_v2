"""diag_five_step.py — why isn't the five-step pipeline starting?
Checks all three layers: config value, main.py wiring, module imports."""

import importlib
import yaml

print("1. CONFIG (as parsed, not as typed):")
cfg = yaml.safe_load(open("config.yaml", encoding="utf-8"))
print("   pipeline =", repr(cfg.get("analytics", {}).get("pipeline")))

print("\n2. MAIN.PY WIRING:")
src = open("main.py", encoding="utf-8").read()
print("   contains 'five_step':", "five_step" in src)
print("   contains 'FiveStepRunner':", "FiveStepRunner" in src)

print("\n3. IMPORT CHAIN:")
for mod in ("ledger", "nlp.engines", "nlp.preprocessor",
            "processed_schema", "nlp.five_step"):
    try:
        importlib.import_module(mod)
        print(f"   {mod:22s} OK")
    except Exception as e:
        print(f"   {mod:22s} FAIL: {type(e).__name__}: {e}")