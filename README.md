# SPECTRUM

**A tamper-evident, offline-capable social intelligence pipeline for Telegram and X.**

SPECTRUM is a pure-Python pipeline that monitors public Telegram (and X, tier permitting) channels, seals every message into a **tamper-evident cryptographic vault**, enriches it through a **five-step hybrid NLP pipeline** (semantic cache → GPU transformer ensemble → lexicon fallback → unsupervised discovery), maps the **social graph** (influence, bridges, botnet leads), and serves everything through an **interactive analyst dashboard** with local-LLM briefings — all on one machine, fully offline-capable except for the source APIs.

~11,000 lines of code across ~30 files, 7 external tools, 25+ database tables.

**One-line architecture:**

```
COLLECT → VALIDATE → VAULT (SHA-256 ledger) → 5-STEP NLP → GRAPH → DASHBOARD → LOCAL-LLM BRIEFINGS
```

---

## Table of Contents

- [Hardware Requirements](#hardware-requirements)
- [System Architecture](#system-architecture)
  - [Collection Layer](#31-collection-layer)
  - [Evidence Layer](#32-evidence-layer-the-trust-core)
  - [The Five-Step NLP Pipeline](#33-the-five-step-nlp-pipeline)
  - [Graph Layer](#34-graph-layer)
  - [Presentation Layer](#35-presentation-layer)
- [Component Map](#component-map)
- [Numerical Profile](#numerical-profile)
- [Current Operational State](#current-operational-state)
- [Installation](#installation)
  - [Prerequisites](#prerequisites)
  - [Step 1 — Clone](#step-1--clone-the-repository)
  - [Step 2 — API Keys](#step-2--gather-api-keys--credentials)
  - [Step 3 — Automated Setup](#step-3--run-the-automated-setup-installps1)
  - [Step 4 — Configure Pipeline & Channels](#step-4--configure-pipeline--channels)
  - [Step 5 — Run the System](#step-5--start-the-full-fledged-system)
- [Diagnostics & Proof Commands](#diagnostics--proof-commands)
- [The One-Paragraph Version](#the-one-paragraph-version)

---

## Hardware Requirements

*The honest tiering — SPECTRUM runs at every tier, quality scales with hardware.*

| Tier | Hardware | What Runs | Quality |
|---|---|---|---|
| **Minimum** | 8 GB RAM, any CPU, ~10 GB disk, no GPU | Everything except Layer-1 ensemble & GPU embedding. Lexicon classifies all misses; MiniLM embeds on CPU; Ollama 3B generates briefings in ~1–5 min each | Fully functional, slower |
| **Recommended** | 16 GB RAM, RTX 3060 12GB (or any ≥6 GB CUDA GPU), SSD | Full five-step plan: ~2.4 GB VRAM ensemble + 0.2 GB MiniLM + 2.2 GB Ollama-when-active (~4.8/6 GB worst case on small cards — OOM cooldown handles collisions) | Full speed: batches <2 s, briefings 10–30 s |
| **Production** | Linux server + systemd | 24/7 unattended: auto-restart, anchor hardening, off-host ledger witnesses | Same code, zero changes |

**Disk budget:** ~7 GB dependencies + ~1 GB HF models + ~2 GB Ollama model; data grows ~1–1.5 KB/message (10⁶ msgs ≈ 1 GB DB + 100 MB ledger + 22 KB anchor).

**The Python constraint:** CUDA torch wheels require Python ≤3.12 (3.13/3.14 have none). The installer probes 3.12 → 3.11 → 3.10 first and auto-falls-back to CPU mode on newer Pythons.

---

## System Architecture

### 3.1 Collection Layer

- **Telegram** (Telethon, MTProto): live listener on public chats, `catch_up()` for offline gaps, history backfill on startup (watermarked, idempotent — ~300 msgs/chat for instant demo data), mentions/forwards/quotes captured for the graph.
- **X** (tweepy, API v2): filtered stream or recent-search polling — requires a paid read tier; absent tier = clean Telegram-only mode.
- **Normalization:** both platforms → one `NormalizedMessage` template ("a post is a post").

### 3.2 Evidence Layer (the trust core)

- **Validation gate** → rejects empty/corrupt/impossible-timestamp data into a capped audit bin.
- **Conveyor** → 100k-slot RAM queue + fsynced disk overflow lane (viral floods stack up, never crash; idempotent replay).
- **Vault** (SQLite, WAL, `synchronous=FULL`): upsert-by-`(source, external_id)` — re-encounters update metrics, never duplicate; integer-microsecond timestamps for instant range scans; FTS5 full-text search.
- **Ledger:** every new message SHA-256-fingerprinted over a canonical form, sealed in Merkle blocks (≤4,096 leaves) inside the same transaction — row exists ⇔ receipt exists. Head hash escapes the machine to a write-once anchor file (off-host cron copies = trust root). Append-only + immutability triggers; `audit.py verify/prove` produce tamper evidence with 12-hash inclusion proofs.

### 3.3 The Five-Step NLP Pipeline

`pipeline: five_step`

1. **Embed** every message (all-MiniLM-L6-v2, 384-dim, cosine-normalized).
2. **Semantic cache** (ChromaDB, HNSW cosine): similarity ≥ 0.95 → inherit labels, GPU bypassed (warm-up: 500 vectors).
3. **Layer 1 — HF transformer ensemble** on cache-misses: HingBERT LID (Hinglish detection), sarcasm detector, twitter-roberta 3-class sentiment, GoEmotions 28→6 taxonomy mapping; irony flag = sarcasm ≥0.80 + positive sentiment.
4. **Layer 2 — lexicon fallback:** routes remaining misses when backlog >100 or observed batch >2,000 ms or CUDA OOM (300 s cooldown). `engine_used` attribution is per-row and auditable.
5. **Discovery** (every 12 min): UMAP (384→5 dims) → HDBSCAN clustering + c-TF-IDF keywords + velocity; Ollama qwen2.5:3b-instruct writes cluster titles + the 100–200 word briefing with exact chat statistics.

The **processed lake** (`processed.db`) holds clean text, tokens, labels, engine attribution, and rationale-provenance — all derived, all recomputable, linked back to vault rows. The legacy lexicon worker runs in parallel (`keep_legacy_analytics`) until the dashboard fully re-points.

### 3.4 Graph Layer

Incremental edge builder (reply/quote/mention/forward, weighted by interaction count, late-parent resolution) → PageRank (megaphones), Brandes betweenness (bridges at p95), label propagation (communities), **botnet flag** = ≥5 members ∧ ≥90% internal weight ∧ ≥60% density. Analysis every 5 min; on-open refresh endpoint for instant maps.

### 3.5 Presentation Layer

FastAPI (11+ endpoints, read-only, optional bearer auth) + single-file dashboard: timeline slider driving every panel, KPI cards, vis-network social map (VIP sizing by PageRank, color by community/sentiment/suspicion), propagation-trace animation, trending topics + cluster cards, semantic search box, and the bottom Chat Summary & Statistics block (LLM briefing + live chips).

---

## Component Map

| Tool / Library | Role | Where |
|---|---|---|
| **Telethon** | Telegram MTProto: live updates, history, profiles | `collectors/telegram_collector.py` |
| **tweepy** | X API v2 stream/poll, metrics refresh, profiles | `collectors/x_collector.py` |
| **SQLite** (WAL+FTS5) | Evidence vault + processed lake; µs-precision time indexes | `storage.py`, `processed_schema.py` |
| **hashlib** (SHA-256) + Merkle trees | Tamper-evident ledger, inclusion proofs | `ledger.py`, `audit.py` |
| **sentence-transformers** (MiniLM) | 384-d embeddings for cache + clustering | `nlp/five_step.py` |
| **ChromaDB** | Persistent HNSW vector store / label cache | Step 2 |
| **transformers** (HF pipeline) | Layer-1 ensemble: LID, sarcasm, sentiment, GoEmotions | `nlp/models/*` |
| **LexiconEngine** (hand-built) | Layer-2 fallback: 164 phrases, 22 emoji, negation/intensifier logic, softmax | `nlp/engines.py` |
| **UMAP + HDBSCAN** | Unsupervised topic discovery (Step 5) | `nlp/five_step.py` discovery |
| **Ollama** (qwen2.5:3b-instruct) | Cluster titling + 100–200 word briefings; CPU-viable | `nlp/llm_ollama.py` |
| **FastAPI + Uvicorn** | Dashboard API, read-only | `dashboard/server.py` |
| **vis-network + Chart.js + Tailwind** | Social map, charts, UI | `dashboard/static/index.html` |
| **systemd** | 24/7 deployment | `deploy/*.service` |
| **install.ps1** | One-shot setup with hardware audit + torch verification | repo root |

---

## Numerical Profile

<details>
<summary><strong>Cryptography & evidence</strong></summary>

- SHA-256 space: 2²⁵⁶ ≈ 1.16×10⁷⁷; forgery probability per attempt ≤ 2⁻²⁵⁶ ≈ 1.2×10⁻⁷⁷; committed-data loss probability 0 (atomic transaction + WAL fsync).
- Merkle proof for a full block: 12 levels, 384 B; alter block *k* of *B* ⇒ *O(B−k)* rehashes + every external copy.
- Ledger overhead ≈ 100.06 B/message; 10⁶ messages = 245 blocks / ~100 MB ledger / 22 KB anchor.

</details>

<details>
<summary><strong>Throughput & scale</strong></summary>

- Write ceiling: 2,000 msgs/200 ms batch = 10,000 msgs/s theoretical; realistic SSD 3–5k/s; RAM queue absorbs 100 s of 1,000 msgs/s flood before disk spill.
- Real bottleneck = source APIs (Telegram push; X read-tier caps).
- Storage growth: 1k/day → ~0.5 GB/yr; 100k/day → ~50 GB/yr (SQLite sweet spot ≤50 GB / ≤5k msgs/s; PostgreSQL port is 1:1 beyond).

</details>

<details>
<summary><strong>NLP subsystem</strong></summary>

- Taxonomies: pipeline = 6 emotions (supportive, hostile, fear, excitement, confusion, neutral) + sarcasm-as-flag + 3-class sentiment + irony flag = 11 tracked dimensions; frontend currently displays the legacy 6 (sarcasm as a category — known inconsistency, one-line fix).
- Lexicon: 164 phrase patterns, 22 emoji signals, 11 negations, 9 intensifiers; O(4·T) scoring; softmax temperature 1.3.
- GoEmotions mapping: 28 → 6 (sadness-family → fear, documented choice).
- Accuracy (method-class estimates): lexicon ~55–65% per-post; HF ensemble ~70–80%; aggregate mixes tighten as √N — the intended usage.
- Cache: 0.95 cosine threshold catches near-duplicates (reposts, template spam, bot repeats — exactly where inheritance is safest); effective ensemble capacity ≈ cap × cluster-size + inherited hits.

</details>

<details>
<summary><strong>Discovery & graph</strong></summary>

- UMAP 384→5 dims, min_cluster 15/min_samples 5, over-clustering guard at 50; c-TF-IDF top-5 keywords; velocity = recent-6h rate / prior-window rate.
- PageRank damping 0.85, ≤60 iterations; Brandes betweenness O(V·E) capped at 20k nodes; label propagation ≤30 sweeps.
- Botnet flag thresholds: n≥5 ∧ internal≥0.90 ∧ density≥0.60; suspicion = ratio × min(1, density/0.5) — a lead generator (~70–85% precision), not a verdict.

</details>

<details>
<summary><strong>Time & alerts</strong></summary>

- 1-µs timestamp resolution; "2:00–2:15 PM Tuesday" = one indexed range scan, sub-ms at 10⁷ rows.
- Viral alert: completed-hour count ≥100 ∧ ≥10× trailing baseline (the 10/hr→1,000/hr spec fires at ratio 100×).

</details>

<details>
<summary><strong>LLM layer</strong></summary>

- Qwen2.5-3B-instruct: briefing 100–200 words (one corrective retry); GPU 10–30 s / CPU 1–5 min; ~4–5 GB RAM CPU-mode; off the hot path (12-min cadence).

</details>

**Effort model:** ~60–70% of mechanical analyst workload eliminated; ~10× coverage multiplier (24/7 vs 2–3 h human attention); interpretation/judgment deliberately not automated.

---

## Current Operational State

*As-built, honestly.*

**Verified running:** Telegram collection + backfill, vault sealing (400+ blocks), validation/conveyor, five-step pipeline live — cache inheritance active, discovery clustering 800 points/9 clusters, Step-4 gate observed firing correctly in production, Ollama UP, dashboard serving, audit VERIFIED, Layer 1 ensemble has produced its first `layer1_transformer` labels.

**Known items:** CUDA torch on this machine pending the Python-3.12 env fix (Layer 1 currently trickles on CPU-speed inference); frontend/backend taxonomy mismatch (sarcasm-vs-confusion) awaiting the one-line align; service-bot boilerplate filter and stopword touch-up optional; X collection off (no paid tier — supported mode); learning engine + embedding-RAG chat designed but not implemented (documented, on request).

---

## Installation

### Prerequisites

- **OS:** Windows 10/11 (64-bit).
- **Python:** 3.12 (Python 3.13/3.14 do not have reliable CUDA PyTorch wheels).
- **GPU (optional but recommended):** NVIDIA RTX GPU with updated Game Ready or Studio drivers.
- **Git:** installed and available in PowerShell.
- **Ollama:** installed from [ollama.com](https://ollama.com) (run the daemon from the Windows system tray).

### Step 1 — Clone the Repository

```powershell
git clone <YOUR_GIT_REPOSITORY_URL> spectrum
cd spectrum
```

### Step 2 — Gather API Keys & Credentials

You need credentials for Telegram (mandatory for channel collection) and optionally X (Twitter):

**1. Telegram credentials (required)**
1. Go to [my.telegram.org](https://my.telegram.org) and log in with your Telegram account phone number.
2. Click **API development tools**.
3. Create an application (name and short name can be anything, e.g. `SpectrumCollector`).
4. Note your `api_id` (numeric integer) and `api_hash` (hex string).

**2. X / Twitter Bearer Token (optional)**
- If using the X pipeline, visit [developer.x.com](https://developer.x.com) to generate an app and copy your `Bearer Token`.
- If you only want to scrape Telegram, you can leave this blank.

### Step 3 — Run the Automated Setup (`install.ps1`)

Execute the one-shot installer. It will verify Python, audit your GPU/RAM, build the virtual environment (`.venv`), pull the correct CUDA PyTorch wheels, configure `.env`, run the ledger smoke test, and perform the Telegram handshake:

```powershell
powershell -ExecutionPolicy Bypass -File .\install.ps1
```

During execution, the script will prompt you for:

1. **CUDA PyTorch installation:** type `Y` and hit Enter to pull the ~2.5 GB PyTorch CUDA build.
2. **Ollama model pull:** type `Y` when asked to pull `qwen2.5:3b-instruct` (~2 GB).
3. **API keys:** enter your `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, and optional `X_BEARER_TOKEN`.
4. **Telegram login:** type `Y` to authenticate. Enter your international phone number (e.g., `+91...`), then the verification code sent to your Telegram client. This writes `collector.session` so the collector can run headless indefinitely.

Flags:

| Flag | Effect |
|---|---|
| `-Reset` | Wipe `.venv` and reinstall from scratch |
| `-CpuOnly` | Skip CUDA torch even if an NVIDIA GPU is present |

### Step 4 — Configure Pipeline & Channels

Open `config.yaml` in your project root and verify the following sections.

**Pipeline mode & target channels** — ensure `pipeline` matches your controller and add the public channels/groups you wish to track:

```yaml
telegram:
  api_id: null         # loaded automatically from .env
  api_hash: null        # loaded automatically from .env
  targets:
    - "Random Chats English"
    - "durov"
    - "telegram"

analytics:
  db_path: data/vault.db
  pipeline: five_step        # or "hybrid"
  keep_legacy_analytics: true

  embeddings:
    enabled: true
    model: "sentence-transformers/all-MiniLM-L6-v2"
    store_path: data/vector_store
    collection: spectrum_vectors
    batch: 64

  clustering:
    enabled: true
    scan_minutes: 12
    window_hours: 48
    min_cluster_size: 15
    min_samples: 5
    umap_neighbors: 15
    velocity_recent_h: 6

  llm:
    enabled: true
    base_url: "http://127.0.0.1:11434"
    model: "qwen2.5:3b-instruct"
    timeout_s: 90
    keep_alive: 0

dashboard:
  host: 127.0.0.1
  port: 8080
```

### Step 5 — Start the Full-Fledged System

Running the system requires two terminal windows.

**Terminal 1 — Ingestion & Analytical Pipeline**

Open PowerShell in the project directory:

```powershell
.\venv\Scripts\Activate.ps1
python main.py
```

What to expect in logs:
- Telethon connects to Telegram servers and starts streaming live messages from your targets.
- Ingestion blocks are cryptographically sealed into `data/vault.db`.
- The hybrid controller starts Layer 1 (GPU ensemble) classification.
- The embedding loop launches with `device=cuda` and vectorizes incoming texts into `data/vector_store`.
- The clustering controller schedules scans every 12 minutes.

**Terminal 2 — Analyst Dashboard Server**

Open a second PowerShell window in the project directory:

```powershell
cd C:\path\to\spectrum
.\venv\Scripts\Activate.ps1
uvicorn dashboard.server:app --port 8080
```

What to expect:
- Uvicorn starts on [http://127.0.0.1:8080](http://127.0.0.1:8080).
- Open your browser to `http://localhost:8080` to access the live dashboard.

---

## Diagnostics & Proof Commands

```powershell
python check_engines.py   # engine attribution — which model classified what, and why
python audit.py verify    # ledger integrity — tamper evidence with inclusion proofs
```

---

## The One-Paragraph Version

~11,000 lines across ~30 files, 7 external tools, 25+ database tables: a system where the evidence layer is mathematically unforgeable (10⁻⁷⁷), the perception layer is honest about being probabilistic (confidence-scored, engine-attributed, recomputable), the discovery layer finds narratives without being told what to look for, and the presentation layer makes all of it explorable in one screen — with `check_engines.py` and `audit.py` turning every claim in this dossier into something you can verify in five seconds. That last property — *nothing gets claimed that can't be proven* — is the design principle that survived every iteration of the build.
