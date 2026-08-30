"""Additive v3 schema (evidence tables untouched). Idempotent — runs at startup."""

import os
import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS analytics_state (name TEXT PRIMARY KEY, value TEXT);

-- 1:1 with messages: analysis results linked to each post
CREATE TABLE IF NOT EXISTS message_analytics (
    message_rowid        INTEGER PRIMARY KEY REFERENCES messages(id),
    analyzed_at_us       INTEGER NOT NULL,
    language             TEXT,
    language_conf        REAL,
    emotions_json        TEXT NOT NULL,          -- {"supportive":12,"hostile":4,...}
    dominant_emotion     TEXT,                   -- incl. 'neutral'
    dominant_emotion_conf REAL,
    topic_key            TEXT,
    author_age_bucket    TEXT,                   -- demographic snapshot AT POST TIME
    author_location      TEXT,
    author_language      TEXT,
    author_interest      TEXT,
    engine               TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_an_dom    ON message_analytics (dominant_emotion);
CREATE INDEX IF NOT EXISTS idx_an_loc    ON message_analytics (author_location);
CREATE INDEX IF NOT EXISTS idx_an_lang   ON message_analytics (author_language);
CREATE INDEX IF NOT EXISTS idx_an_topic  ON message_analytics (topic_key);

CREATE TABLE IF NOT EXISTS author_profiles (
    source TEXT NOT NULL, author_id TEXT NOT NULL,
    username TEXT, bio TEXT, location TEXT, verified INTEGER,
    fetched_at_us INTEGER NOT NULL, revision INTEGER NOT NULL DEFAULT 1, raw_json TEXT,
    PRIMARY KEY (source, author_id)
);
CREATE TABLE IF NOT EXISTS author_demographics (
    source TEXT NOT NULL, author_id TEXT NOT NULL,
    profile_revision INTEGER NOT NULL DEFAULT 0,
    age_bucket TEXT, age_conf REAL,
    location TEXT, location_conf REAL,
    language TEXT, language_conf REAL,
    interest TEXT, interest_conf REAL,
    evidence_json TEXT, inferred_at_us INTEGER NOT NULL,
    PRIMARY KEY (source, author_id)
);
CREATE TABLE IF NOT EXISTS author_demographics_history (   -- who was talking, when
    source TEXT, author_id TEXT, inferred_at_us INTEGER,
    age_bucket TEXT, location TEXT, language TEXT, interest TEXT
);

CREATE TABLE IF NOT EXISTS term_hourly (
    term TEXT NOT NULL, hour_us INTEGER NOT NULL, n INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (term, hour_us)
);
CREATE TABLE IF NOT EXISTS topics (
    topic_key TEXT PRIMARY KEY, label TEXT, signature_json TEXT,
    first_seen_us INTEGER, last_scan_us INTEGER, active INTEGER DEFAULT 1
);
CREATE TABLE IF NOT EXISTS topic_hourly (
    topic_key TEXT NOT NULL, hour_us INTEGER NOT NULL, n INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (topic_key, hour_us)
);
CREATE TABLE IF NOT EXISTS message_topics (
    message_rowid INTEGER PRIMARY KEY, topic_key TEXT, matched_json TEXT, assigned_at_us INTEGER
);
CREATE TABLE IF NOT EXISTS viral_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic_key TEXT NOT NULL, hour_us INTEGER NOT NULL, triggered_at_us INTEGER NOT NULL,
    prev_rate REAL, new_rate REAL, ratio REAL,
    UNIQUE (topic_key, hour_us)                -- one alert per topic per hour
);

CREATE TABLE IF NOT EXISTS graph_nodes (
    source TEXT NOT NULL, author_id TEXT NOT NULL, username TEXT,
    first_us INTEGER NOT NULL, last_us INTEGER NOT NULL, message_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source, author_id)
);
CREATE TABLE IF NOT EXISTS graph_edges (
    source TEXT NOT NULL, kind TEXT NOT NULL,          -- reply | mention | quote | forward
    from_author TEXT NOT NULL, to_author TEXT NOT NULL,
    weight INTEGER NOT NULL DEFAULT 1,                 -- the "thicker line"
    first_us INTEGER NOT NULL, last_us INTEGER NOT NULL,
    PRIMARY KEY (source, kind, from_author, to_author)
);
CREATE INDEX IF NOT EXISTS idx_edges_to   ON graph_edges (source, to_author);
CREATE INDEX IF NOT EXISTS idx_edges_last ON graph_edges (last_us);
CREATE TABLE IF NOT EXISTS pending_edges (             -- reply/quote whose parent isn't stored yet
    message_rowid INTEGER PRIMARY KEY, source TEXT, kind TEXT, external_id TEXT, at_us INTEGER
);

CREATE TABLE IF NOT EXISTS graph_metrics (
    source TEXT NOT NULL, author_id TEXT NOT NULL,
    in_w REAL, out_w REAL, pagerank REAL, betweenness REAL,
    community INTEGER, bridge INTEGER DEFAULT 0, computed_at_us INTEGER NOT NULL,
    PRIMARY KEY (source, author_id)
);
CREATE TABLE IF NOT EXISTS communities (
    source TEXT NOT NULL, community INTEGER NOT NULL,
    size INTEGER, internal_w REAL, external_w REAL, internal_ratio REAL,
    density REAL, suspicion REAL, flagged INTEGER DEFAULT 0, computed_at_us INTEGER,
    PRIMARY KEY (source, community)
);
"""


def analytics_connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None,
                           check_same_thread=False)
    for p in ("PRAGMA journal_mode=WAL", "PRAGMA busy_timeout=30000",
              "PRAGMA synchronous=NORMAL", "PRAGMA cache_size=-32768"):
        conn.execute(p)
    return conn


def ensure_schema(db_path: str) -> None:
    conn = analytics_connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


def get_state(conn, name: str, default: str = "0") -> str:
    row = conn.execute("SELECT value FROM analytics_state WHERE name=?", (name,)).fetchone()
    return row[0] if row else default


def set_state(conn, name: str, value) -> None:
    conn.execute("INSERT INTO analytics_state (name, value) VALUES (?,?) "
                 "ON CONFLICT(name) DO UPDATE SET value=excluded.value", (name, str(value)))