"""Processed lake schema (data/processed.db). Derivative analytics ONLY —
the raw evidence never leaves data/vault.db, and every row links back via
raw_message_id. Fully recomputable from the vault at any time."""

import sqlite3
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS processed_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_message_id INTEGER UNIQUE NOT NULL,  -- -> vault.db:messages.id
    source TEXT NOT NULL,
    author_id TEXT NOT NULL,
    posted_at_us INTEGER NOT NULL,
    clean_text TEXT NOT NULL,
    filtered_tokens TEXT NOT NULL,           -- JSON array (lemmatized, stopword-pruned)
    engine_used TEXT NOT NULL,               -- 'layer1_transformer' | 'layer2_lexicon'
    language_label TEXT,                     -- 'en' | 'hinglish' | builtin codes
    is_sarcastic INTEGER,
    sarcasm_conf REAL,
    irony_flag INTEGER DEFAULT 0,            -- blueprint: sarcasm>thr AND positive sentiment
    sentiment_label TEXT NOT NULL,           -- 'positive' | 'negative' | 'neutral'
    sentiment_conf REAL NOT NULL,
    dominant_emotion TEXT NOT NULL,          -- supportive|hostile|fear|excitement|confusion|neutral
    emotion_conf REAL NOT NULL,
    processed_at_us INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_proc_raw      ON processed_messages(raw_message_id);
CREATE INDEX IF NOT EXISTS idx_proc_engine         ON processed_messages(engine_used);
CREATE INDEX IF NOT EXISTS idx_proc_sentiment     ON processed_messages(sentiment_label);
CREATE INDEX IF NOT EXISTS idx_proc_emotion       ON processed_messages(dominant_emotion);
CREATE INDEX IF NOT EXISTS idx_proc_posted        ON processed_messages(posted_at_us);

CREATE TABLE IF NOT EXISTS topic_clusters (       -- Phase 5 writes here; created now
    cluster_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    top_keywords TEXT NOT NULL,
    exemplar_text TEXT NOT NULL,
    message_count INTEGER NOT NULL,
    dominant_sentiment TEXT NOT NULL,
    dominant_emotion TEXT NOT NULL,
    sarcasm_rate REAL NOT NULL,
    velocity_score REAL NOT NULL,
    last_updated_us INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS hybrid_state (         -- routing telemetry for the dashboard
    key TEXT PRIMARY KEY, value TEXT
);
"""


def processed_connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=5, isolation_level=None,
                           check_same_thread=False)
    return conn


def ensure_processed_schema(db_path: str) -> None:
    conn = processed_connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()