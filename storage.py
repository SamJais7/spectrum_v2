# """The vault: one universal schema, time-first integer-µs indexes, idempotent
# upserts (metrics update — never a duplicate row), conversation graph, and the
# append-only ledger sealed atomically with every batch."""

# import json
# import logging
# import os
# import sqlite3
# import threading
# from collections import Counter
# from dataclasses import dataclass
# from pathlib import Path

# from ledger import Ledger, message_hash, epoch_us, now_us
# from models import NormalizedMessage

# log = logging.getLogger("collector.vault")

# SCHEMA = """
# CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT);

# CREATE TABLE IF NOT EXISTS messages (
#     id                   INTEGER PRIMARY KEY AUTOINCREMENT,
#     source               TEXT    NOT NULL CHECK (source IN ('x','telegram')),
#     external_id          TEXT    NOT NULL,
#     conversation_id      TEXT,
#     author_id            TEXT,
#     author_username      TEXT,
#     text                 TEXT    NOT NULL,
#     posted_at_us         INTEGER NOT NULL,   -- publisher clock, UTC epoch µs
#     ingested_at_us       INTEGER NOT NULL,   -- our clock at first receipt
#     last_seen_at_us      INTEGER NOT NULL,   -- our clock at latest re-encounter
#     reply_to_external_id TEXT,
#     metrics_json         TEXT    NOT NULL DEFAULT '{}',
#     content_hash         TEXT    NOT NULL,   -- ledger fingerprint (identity+content)
#     revision             INTEGER NOT NULL DEFAULT 1,  -- +1 per metrics update
#     UNIQUE (source, external_id)             -- the no-duplicates guarantee
# );
# CREATE INDEX IF NOT EXISTS idx_messages_posted ON messages (posted_at_us);
# CREATE INDEX IF NOT EXISTS idx_messages_ingested ON messages (ingested_at_us);
# CREATE INDEX IF NOT EXISTS idx_messages_conv   ON messages (conversation_id, posted_at_us);
# CREATE INDEX IF NOT EXISTS idx_messages_author ON messages (source, author_id);
# CREATE INDEX IF NOT EXISTS idx_messages_child  ON messages (source, reply_to_external_id);
# CREATE INDEX IF NOT EXISTS idx_messages_hash   ON messages (content_hash);

# CREATE TABLE IF NOT EXISTS authors (
#     source TEXT NOT NULL, author_id TEXT NOT NULL, username TEXT,
#     first_seen_us INTEGER NOT NULL, last_seen_us INTEGER NOT NULL,
#     message_count INTEGER NOT NULL DEFAULT 0,
#     PRIMARY KEY (source, author_id)
# );
# CREATE TABLE IF NOT EXISTS conversations (
#     source TEXT NOT NULL, conversation_id TEXT NOT NULL,
#     first_seen_us INTEGER NOT NULL, last_seen_us INTEGER NOT NULL,
#     message_count INTEGER NOT NULL DEFAULT 0,
#     PRIMARY KEY (source, conversation_id)
# );
# CREATE TABLE IF NOT EXISTS activity_hours (      -- coarse timeline rollup
#     hour_us INTEGER NOT NULL, source TEXT NOT NULL, n INTEGER NOT NULL DEFAULT 0,
#     PRIMARY KEY (hour_us, source)
# );
# CREATE TABLE IF NOT EXISTS engagement_snapshots (
#     id INTEGER PRIMARY KEY AUTOINCREMENT,
#     message_rowid INTEGER NOT NULL,
#     captured_at_us INTEGER NOT NULL,
#     revision INTEGER NOT NULL,
#     metrics_json TEXT NOT NULL
# );
# CREATE INDEX IF NOT EXISTS idx_snaps_rowid ON engagement_snapshots (message_rowid);

# CREATE TABLE IF NOT EXISTS rejected (            -- why the gate said no (capped)
#     id INTEGER PRIMARY KEY AUTOINCREMENT,
#     at_us INTEGER NOT NULL, source TEXT, external_id TEXT,
#     reason TEXT NOT NULL, snippet TEXT
# );

# -- conversation graph: child → parent, resolved by (source, external_id).
# -- Resolves automatically even when a parent arrives AFTER its reply.
# CREATE VIEW IF NOT EXISTS thread_edges AS
# SELECT c.id AS child_id, p.id AS parent_id, c.source AS source
# FROM messages c
# JOIN messages p ON p.source = c.source AND p.external_id = c.reply_to_external_id;

# -- the receipt book (append-only, guarded by triggers)
# CREATE TABLE IF NOT EXISTS ledger_blocks (
#     seq INTEGER PRIMARY KEY,
#     prev_hash TEXT NOT NULL, merkle_root TEXT NOT NULL,
#     message_count INTEGER NOT NULL,
#     first_rowid INTEGER NOT NULL, last_rowid INTEGER NOT NULL,
#     sealed_at_us INTEGER NOT NULL, block_hash TEXT NOT NULL
# );
# CREATE TABLE IF NOT EXISTS ledger_entries (
#     block_seq INTEGER NOT NULL REFERENCES ledger_blocks(seq),
#     pos INTEGER NOT NULL, message_rowid INTEGER NOT NULL, message_hash TEXT NOT NULL,
#     PRIMARY KEY (block_seq, pos)
# );
# CREATE INDEX IF NOT EXISTS idx_entries_rowid ON ledger_entries (message_rowid);

# CREATE TRIGGER IF NOT EXISTS trg_blocks_no_update  BEFORE UPDATE ON ledger_blocks
# BEGIN SELECT RAISE(ABORT, 'ledger_blocks is append-only'); END;
# CREATE TRIGGER IF NOT EXISTS trg_blocks_no_delete  BEFORE DELETE ON ledger_blocks
# BEGIN SELECT RAISE(ABORT, 'ledger_blocks is append-only'); END;
# CREATE TRIGGER IF NOT EXISTS trg_entries_no_update BEFORE UPDATE ON ledger_entries
# BEGIN SELECT RAISE(ABORT, 'ledger_entries is append-only'); END;
# CREATE TRIGGER IF NOT EXISTS trg_entries_no_delete BEFORE DELETE ON ledger_entries
# BEGIN SELECT RAISE(ABORT, 'ledger_entries is append-only'); END;

# -- full-text search over (immutable) text
# CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5 (text, content='messages', content_rowid='id');
# CREATE TRIGGER IF NOT EXISTS messages_fts_ai AFTER INSERT ON messages BEGIN
#     INSERT INTO messages_fts (rowid, text) VALUES (new.id, new.text);
# END;

# -- human-readable timestamps for ad-hoc queries
# CREATE VIEW IF NOT EXISTS messages_t AS
# SELECT id, source, external_id, conversation_id, author_id, author_username,
#        datetime(posted_at_us/1000000, 'unixepoch') AS posted_at,
#        datetime(ingested_at_us/1000000, 'unixepoch') AS ingested_at,
#        revision, metrics_json, text
# FROM messages;
# """

# IMMUTABLE_TRIGGERS = """
# CREATE TRIGGER IF NOT EXISTS trg_messages_content_immutable BEFORE UPDATE ON messages
# BEGIN
#     SELECT RAISE(ABORT, 'message content is immutable (only metrics may change)')
#     WHERE OLD.source <> NEW.source OR OLD.external_id <> NEW.external_id
#        OR OLD.conversation_id <> NEW.conversation_id OR OLD.author_id <> NEW.author_id
#        OR OLD.author_username <> NEW.author_username OR OLD.text <> NEW.text
#        OR OLD.posted_at_us <> NEW.posted_at_us OR OLD.ingested_at_us <> NEW.ingested_at_us
#        OR OLD.reply_to_external_id <> NEW.reply_to_external_id
#        OR OLD.content_hash <> NEW.content_hash;
# END;
# CREATE TRIGGER IF NOT EXISTS trg_messages_no_delete BEFORE DELETE ON messages
# BEGIN SELECT RAISE(ABORT, 'messages cannot be deleted (enforce_immutable_messages=true)'); END;
# """


# @dataclass
# class BatchResult:
#     inserted: int = 0
#     updated: int = 0      # metrics refreshed by re-encounter
#     unchanged: int = 0
#     blocks: int = 0
#     last_block_seq: int = 0


# def _metrics_json(metrics: dict) -> str:
#     return json.dumps(metrics, sort_keys=True, ensure_ascii=False, default=str)


# class Vault:
#     def __init__(self, cfg: dict, ledger: Ledger):
#         self.cfg = cfg
#         self.ledger = ledger
#         self._db_path = cfg["db_path"]
#         Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
#         self._lock = threading.RLock()
#         self._conn = sqlite3.connect(self._db_path, check_same_thread=False,
#                                      isolation_level=None, timeout=30)
#         for p in ("PRAGMA journal_mode=WAL",
#                   "PRAGMA synchronous=FULL",      # every commit is power-cut safe
#                   "PRAGMA busy_timeout=30000",
#                   "PRAGMA cache_size=-65536",     # 64 MiB page cache
#                   "PRAGMA wal_autocheckpoint=2000",
#                   "PRAGMA temp_store=MEMORY"):
#             self._conn.execute(p)
#         with self._lock:
#             self._conn.executescript(SCHEMA)
#             if cfg.get("security", {}).get("enforce_immutable_messages", True):
#                 self._conn.executescript(IMMUTABLE_TRIGGERS)
#             else:
#                 for t in ("trg_messages_content_immutable", "trg_messages_no_delete"):
#                     self._conn.execute(f"DROP TRIGGER IF EXISTS {t}")
#             self._conn.execute("INSERT OR REPLACE INTO schema_meta VALUES ('schema_version','2')")
#             self.ledger.ensure_genesis(self._conn)
#         os.chmod(self._db_path, 0o600)
#         self.ledger.reconcile_anchor(self._conn)

#     # ------------------------------------------------------------ ingest

#     def write_batch(self, items) -> BatchResult:
#         """All-or-nothing: messages + graph counters + ledger receipts commit
#         atomically. Idempotent: re-encounters update metrics, never duplicate.
#         Items may be NormalizedMessage or (message, received_at_us) — the
#         tuple form preserves an explicit receipt time (used for re-ingestion)."""
#         if not items:
#             return BatchResult()
#         now = now_us()
#         res, entries, sealed = BatchResult(), [], []
#         authors, convs, hours = Counter(), Counter(), Counter()
#         with self._lock:
#             conn = self._conn
#             try:
#                 conn.execute("BEGIN IMMEDIATE")
#                 for item in items:
#                     recv = now
#                     m = item
#                     if isinstance(item, tuple):
#                         m, recv = item
#                     h = message_hash(m)
#                     p_us = epoch_us(m.posted_at)
#                     mj = _metrics_json(m.metrics)
#                     row = conn.execute(
#                         "SELECT id, metrics_json FROM messages WHERE source=? AND external_id=?",
#                         (m.source, m.external_id)).fetchone()
#                     if row is None:                                   # first sighting
#                         cur = conn.execute(
#                             """INSERT INTO messages
#                                  (source, external_id, conversation_id, author_id,
#                                   author_username, text, posted_at_us, ingested_at_us,
#                                   last_seen_at_us, reply_to_external_id, metrics_json,
#                                   content_hash, revision)
#                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1)""",
#                             (m.source, m.external_id, m.conversation_id, m.author_id,
#                              m.author_username, m.text, p_us, recv, recv,
#                              m.reply_to_external_id, mj, h))
#                         entries.append((cur.lastrowid, h))             # receipt
#                         res.inserted += 1
#                         if m.author_id:
#                             authors[(m.source, m.author_id, m.author_username)] += 1
#                         if m.conversation_id:
#                             convs[(m.source, m.conversation_id)] += 1
#                         hours[(p_us // 3_600_000_000) * 3_600_000_000, m.source] += 1
#                     else:                                              # seen before
#                         rowid, old_mj = row
#                         if old_mj != mj:                               # stats changed
#                             conn.execute("UPDATE messages SET metrics_json=?,"
#                                          " last_seen_at_us=?, revision=revision+1 WHERE id=?",
#                                          (mj, recv, rowid))
#                             conn.execute("INSERT INTO engagement_snapshots (message_rowid,"
#                                          " captured_at_us, revision, metrics_json)"
#                                          " VALUES (?,?,(SELECT revision FROM messages WHERE id=?),?)",
#                                          (rowid, recv, rowid, mj))
#                             res.updated += 1
#                         else:
#                             res.unchanged += 1                         # pure duplicate
#                 self._bump(authors, convs, hours, now)
#                 if entries:
#                     sealed = self.ledger.seal(conn, entries)
#                     res.blocks, res.last_block_seq = len(sealed), sealed[-1][0]
#                 conn.execute("COMMIT")
#             except BaseException:
#                 try:
#                     conn.execute("ROLLBACK")
#                 except sqlite3.Error:
#                     pass
#                 raise
#         if sealed:
#             self.ledger.anchor(res.last_block_seq, sealed[-1][1])
#         return res

#     def _bump(self, authors, convs, hours, now) -> None:
#         conn = self._conn
#         for (source, author_id, username), n in authors.items():
#             conn.execute("""INSERT INTO authors (source, author_id, username,
#                                first_seen_us, last_seen_us, message_count) VALUES (?,?,?,?,?,?)
#                             ON CONFLICT(source, author_id) DO UPDATE SET username=excluded.username,
#                                last_seen_us=excluded.last_seen_us,
#                                message_count=authors.message_count+excluded.message_count""",
#                          (source, author_id, username, now, now, n))
#         for (source, cid), n in convs.items():
#             conn.execute("""INSERT INTO conversations (source, conversation_id,
#                                first_seen_us, last_seen_us, message_count) VALUES (?,?,?,?,?)
#                             ON CONFLICT(source, conversation_id) DO UPDATE SET
#                                last_seen_us=excluded.last_seen_us,
#                                message_count=conversations.message_count+excluded.message_count""",
#                          (source, cid, now, now, n))
#         for (hour_us, source), n in hours.items():
#             conn.execute("INSERT INTO activity_hours (hour_us, source, n) VALUES (?,?,?) "
#                          "ON CONFLICT(hour_us, source) DO UPDATE SET n=activity_hours.n+excluded.n",
#                          (hour_us, source, n))

#     def record_rejection(self, m, reason: str) -> None:
#         try:
#             with self._lock:
#                 self._conn.execute("BEGIN IMMEDIATE")
#                 self._conn.execute("INSERT INTO rejected (at_us, source, external_id, reason,"
#                                    " snippet) VALUES (?,?,?,?,?)",
#                                    (now_us(), getattr(m, "source", None),
#                                     getattr(m, "external_id", None), reason[:200],
#                                     (getattr(m, "text", None) or "")[:200]))
#                 self._conn.execute("DELETE FROM rejected WHERE id < (SELECT COALESCE(MAX(id),0)"
#                                    " FROM rejected) - ?",
#                                    (int(self.cfg.get("rejected_retention", 10000)),))
#                 self._conn.execute("COMMIT")
#         except Exception:
#             try:
#                 self._conn.execute("ROLLBACK")
#             except sqlite3.Error:
#                 pass
#             log.exception("could not record rejection")

#     # ------------------------------------------------------- reads / ops

#     def latest_external_id(self, source):
#         with self._lock:
#             row = self._conn.execute("SELECT external_id FROM messages WHERE source=? "
#                                      "ORDER BY CAST(external_id AS INTEGER) DESC LIMIT 1",
#                                      (source,)).fetchone()
#         return row[0] if row else None

#     def recent_x_rows(self, limit=100):
#         with self._lock:
#             return self._conn.execute("SELECT id, external_id FROM messages WHERE source='x' "
#                                       "ORDER BY id DESC LIMIT ?", (limit,)).fetchall()

#     def refresh_metrics(self, message_rowid: int, metrics: dict) -> bool:
#         """Used by the X metrics refresher: update stats + snapshot if changed."""
#         mj = _metrics_json(metrics)
#         with self._lock:
#             row = self._conn.execute("SELECT metrics_json FROM messages WHERE id=?",
#                                      (message_rowid,)).fetchone()
#             if not row or row[0] == mj:
#                 return False
#             now = now_us()
#             try:
#                 self._conn.execute("BEGIN IMMEDIATE")
#                 self._conn.execute("UPDATE messages SET metrics_json=?, last_seen_at_us=?,"
#                                    " revision=revision+1 WHERE id=?", (mj, now, message_rowid))
#                 self._conn.execute("INSERT INTO engagement_snapshots (message_rowid,"
#                                    " captured_at_us, revision, metrics_json)"
#                                    " VALUES (?,?,(SELECT revision FROM messages WHERE id=?),?)",
#                                    (message_rowid, now, message_rowid, mj))
#                 self._conn.execute("COMMIT")
#                 return True
#             except BaseException:
#                 try:
#                     self._conn.execute("ROLLBACK")
#                 except sqlite3.Error:
#                     pass
#                 raise

#     def search(self, query: str, limit: int = 50):
#         with self._lock:
#             return self._conn.execute(
#                 "SELECT m.source, m.author_username, m.posted_at_us, m.text "
#                 "FROM messages m JOIN messages_fts f ON f.rowid = m.id "
#                 "WHERE messages_fts MATCH ? ORDER BY m.ingested_at_us DESC LIMIT ?",
#                 (query, limit)).fetchall()

#     def close(self):
#         with self._lock:
#             self._conn.close()


"""The vault: one universal schema, time-first integer-µs indexes, idempotent
upserts (metrics update — never a duplicate row), conversation graph, and the
append-only ledger sealed atomically with every batch."""

import json
import logging
import os
import sqlite3
import threading
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from ledger import Ledger, message_hash, epoch_us, now_us
from models import NormalizedMessage

log = logging.getLogger("collector.vault")

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT);

CREATE TABLE IF NOT EXISTS messages (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    source               TEXT    NOT NULL CHECK (source IN ('x','telegram')),
    external_id          TEXT    NOT NULL,
    conversation_id      TEXT,
    author_id            TEXT,
    author_username      TEXT,
    text                 TEXT    NOT NULL,
    posted_at_us         INTEGER NOT NULL,   -- publisher clock, UTC epoch µs
    ingested_at_us       INTEGER NOT NULL,   -- our clock at first receipt
    last_seen_at_us      INTEGER NOT NULL,   -- our clock at latest re-encounter
    reply_to_external_id TEXT,
    raw_json             TEXT,
    metrics_json         TEXT    NOT NULL DEFAULT '{}',
    content_hash         TEXT    NOT NULL,   -- ledger fingerprint (identity+content)
    revision             INTEGER NOT NULL DEFAULT 1,  -- +1 per metrics update
    UNIQUE (source, external_id)             -- the no-duplicates guarantee
);
CREATE INDEX IF NOT EXISTS idx_messages_posted ON messages (posted_at_us);
CREATE INDEX IF NOT EXISTS idx_messages_ingested ON messages (ingested_at_us);
CREATE INDEX IF NOT EXISTS idx_messages_conv   ON messages (conversation_id, posted_at_us);
CREATE INDEX IF NOT EXISTS idx_messages_author ON messages (source, author_id);
CREATE INDEX IF NOT EXISTS idx_messages_child  ON messages (source, reply_to_external_id);
CREATE INDEX IF NOT EXISTS idx_messages_hash   ON messages (content_hash);

CREATE TABLE IF NOT EXISTS authors (
    source TEXT NOT NULL, author_id TEXT NOT NULL, username TEXT,
    first_seen_us INTEGER NOT NULL, last_seen_us INTEGER NOT NULL,
    message_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source, author_id)
);
CREATE TABLE IF NOT EXISTS conversations (
    source TEXT NOT NULL, conversation_id TEXT NOT NULL,
    first_seen_us INTEGER NOT NULL, last_seen_us INTEGER NOT NULL,
    message_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source, conversation_id)
);
CREATE TABLE IF NOT EXISTS activity_hours (      -- coarse timeline rollup
    hour_us INTEGER NOT NULL, source TEXT NOT NULL, n INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (hour_us, source)
);
CREATE TABLE IF NOT EXISTS engagement_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_rowid INTEGER NOT NULL,
    captured_at_us INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    metrics_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_snaps_rowid ON engagement_snapshots (message_rowid);

CREATE TABLE IF NOT EXISTS rejected (            -- why the gate said no (capped)
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at_us INTEGER NOT NULL, source TEXT, external_id TEXT,
    reason TEXT NOT NULL, snippet TEXT
);

-- conversation graph: child → parent, resolved by (source, external_id).
-- Resolves automatically even when a parent arrives AFTER its reply.
CREATE VIEW IF NOT EXISTS thread_edges AS
SELECT c.id AS child_id, p.id AS parent_id, c.source AS source
FROM messages c
JOIN messages p ON p.source = c.source AND p.external_id = c.reply_to_external_id;

-- the receipt book (append-only, guarded by triggers)
CREATE TABLE IF NOT EXISTS ledger_blocks (
    seq INTEGER PRIMARY KEY,
    prev_hash TEXT NOT NULL, merkle_root TEXT NOT NULL,
    message_count INTEGER NOT NULL,
    first_rowid INTEGER NOT NULL, last_rowid INTEGER NOT NULL,
    sealed_at_us INTEGER NOT NULL, block_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ledger_entries (
    block_seq INTEGER NOT NULL REFERENCES ledger_blocks(seq),
    pos INTEGER NOT NULL, message_rowid INTEGER NOT NULL, message_hash TEXT NOT NULL,
    PRIMARY KEY (block_seq, pos)
);
CREATE INDEX IF NOT EXISTS idx_entries_rowid ON ledger_entries (message_rowid);

CREATE TRIGGER IF NOT EXISTS trg_blocks_no_update  BEFORE UPDATE ON ledger_blocks
BEGIN SELECT RAISE(ABORT, 'ledger_blocks is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_blocks_no_delete  BEFORE DELETE ON ledger_blocks
BEGIN SELECT RAISE(ABORT, 'ledger_blocks is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_entries_no_update BEFORE UPDATE ON ledger_entries
BEGIN SELECT RAISE(ABORT, 'ledger_entries is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_entries_no_delete BEFORE DELETE ON ledger_entries
BEGIN SELECT RAISE(ABORT, 'ledger_entries is append-only'); END;

-- full-text search over (immutable) text
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5 (text, content='messages', content_rowid='id');
CREATE TRIGGER IF NOT EXISTS messages_fts_ai AFTER INSERT ON messages BEGIN
    INSERT INTO messages_fts (rowid, text) VALUES (new.id, new.text);
END;

-- human-readable timestamps for ad-hoc queries
CREATE VIEW IF NOT EXISTS messages_t AS
SELECT id, source, external_id, conversation_id, author_id, author_username,
       datetime(posted_at_us/1000000, 'unixepoch') AS posted_at,
       datetime(ingested_at_us/1000000, 'unixepoch') AS ingested_at,
       revision, metrics_json, text
FROM messages;
"""

IMMUTABLE_TRIGGERS = """
CREATE TRIGGER IF NOT EXISTS trg_messages_content_immutable BEFORE UPDATE ON messages
BEGIN
    SELECT RAISE(ABORT, 'message content is immutable (only metrics may change)')
    WHERE OLD.source <> NEW.source OR OLD.external_id <> NEW.external_id
       OR OLD.conversation_id <> NEW.conversation_id OR OLD.author_id <> NEW.author_id
       OR OLD.author_username <> NEW.author_username OR OLD.text <> NEW.text
       OR OLD.posted_at_us <> NEW.posted_at_us OR OLD.ingested_at_us <> NEW.ingested_at_us
       OR OLD.reply_to_external_id <> NEW.reply_to_external_id
       OR OLD.content_hash <> NEW.content_hash;
END;
CREATE TRIGGER IF NOT EXISTS trg_messages_no_delete BEFORE DELETE ON messages
BEGIN SELECT RAISE(ABORT, 'messages cannot be deleted (enforce_immutable_messages=true)'); END;
"""


@dataclass
class BatchResult:
    inserted: int = 0
    updated: int = 0      # metrics refreshed by re-encounter
    unchanged: int = 0
    blocks: int = 0
    last_block_seq: int = 0


def _metrics_json(metrics: dict) -> str:
    return json.dumps(metrics, sort_keys=True, ensure_ascii=False, default=str)


class Vault:
    def __init__(self, cfg: dict, ledger: Ledger):
        self.cfg = cfg
        self.ledger = ledger
        self._db_path = cfg["db_path"]
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False,
                                     isolation_level=None, timeout=30)
        for p in ("PRAGMA journal_mode=WAL",
                  "PRAGMA synchronous=FULL",      # every commit is power-cut safe
                  "PRAGMA busy_timeout=30000",
                  "PRAGMA cache_size=-65536",     # 64 MiB page cache
                  "PRAGMA wal_autocheckpoint=2000",
                  "PRAGMA temp_store=MEMORY"):
            self._conn.execute(p)
        with self._lock:
            self._conn.executescript(SCHEMA)
            if cfg.get("security", {}).get("enforce_immutable_messages", True):
                self._conn.executescript(IMMUTABLE_TRIGGERS)
            else:
                for t in ("trg_messages_content_immutable", "trg_messages_no_delete"):
                    self._conn.execute(f"DROP TRIGGER IF EXISTS {t}")
            self._conn.execute("INSERT OR REPLACE INTO schema_meta VALUES ('schema_version','2')")
            self.ledger.ensure_genesis(self._conn)
        os.chmod(self._db_path, 0o600)
        self.ledger.reconcile_anchor(self._conn)

    # ------------------------------------------------------------ ingest

    def write_batch(self, items) -> BatchResult:
        """All-or-nothing: messages + graph counters + ledger receipts commit
        atomically. Idempotent: re-encounters update metrics, never duplicate.
        Items may be NormalizedMessage or (message, received_at_us) — the
        tuple form preserves an explicit receipt time (used for re-ingestion)."""
        if not items:
            return BatchResult()
        now = now_us()
        res, entries, sealed = BatchResult(), [], []
        authors, convs, hours = Counter(), Counter(), Counter()
        with self._lock:
            conn = self._conn
            try:
                conn.execute("BEGIN IMMEDIATE")
                for item in items:
                    recv = now
                    m = item
                    if isinstance(item, tuple):
                        m, recv = item
                    h = message_hash(m)
                    p_us = epoch_us(m.posted_at)
                    mj = _metrics_json(m.metrics)
                    raw_j = getattr(m, "raw_json", None)
                    if raw_j is not None and not isinstance(raw_j, str):
                        raw_j = json.dumps(raw_j, ensure_ascii=False, default=str)

                    row = conn.execute(
                        "SELECT id, metrics_json FROM messages WHERE source=? AND external_id=?",
                        (m.source, m.external_id)).fetchone()
                    if row is None:                                   # first sighting
                        cur = conn.execute(
                            """INSERT INTO messages
                                 (source, external_id, conversation_id, author_id,
                                  author_username, text, posted_at_us, ingested_at_us,
                                  last_seen_at_us, reply_to_external_id, raw_json, metrics_json,
                                  content_hash, revision)
                               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
                            (m.source, m.external_id, m.conversation_id, m.author_id,
                             m.author_username, m.text, p_us, recv, recv,
                             m.reply_to_external_id, raw_j, mj, h))
                        entries.append((cur.lastrowid, h))             # receipt
                        res.inserted += 1
                        if m.author_id:
                            authors[(m.source, m.author_id, m.author_username)] += 1
                        if m.conversation_id:
                            convs[(m.source, m.conversation_id)] += 1
                        hours[(p_us // 3_600_000_000) * 3_600_000_000, m.source] += 1
                    else:                                             # seen before
                        rowid, old_mj = row
                        if old_mj != mj:                               # stats changed
                            conn.execute("UPDATE messages SET metrics_json=?,"
                                         " last_seen_at_us=?, revision=revision+1 WHERE id=?",
                                         (mj, recv, rowid))
                            conn.execute("INSERT INTO engagement_snapshots (message_rowid,"
                                         " captured_at_us, revision, metrics_json)"
                                         " VALUES (?,?,(SELECT revision FROM messages WHERE id=?),?)",
                                         (rowid, recv, rowid, mj))
                            res.updated += 1
                        else:
                            res.unchanged += 1                         # pure duplicate
                self._bump(authors, convs, hours, now)
                if entries:
                    sealed = self.ledger.seal(conn, entries)
                    res.blocks, res.last_block_seq = len(sealed), sealed[-1][0]
                conn.execute("COMMIT")
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
        if sealed:
            self.ledger.anchor(res.last_block_seq, sealed[-1][1])
        return res

    def _bump(self, authors, convs, hours, now) -> None:
        conn = self._conn
        for (source, author_id, username), n in authors.items():
            conn.execute("""INSERT INTO authors (source, author_id, username,
                               first_seen_us, last_seen_us, message_count) VALUES (?,?,?,?,?,?)
                            ON CONFLICT(source, author_id) DO UPDATE SET username=excluded.username,
                               last_seen_us=excluded.last_seen_us,
                               message_count=authors.message_count+excluded.message_count""",
                         (source, author_id, username, now, now, n))
        for (source, cid), n in convs.items():
            conn.execute("""INSERT INTO conversations (source, conversation_id,
                               first_seen_us, last_seen_us, message_count) VALUES (?,?,?,?,?)
                            ON CONFLICT(source, conversation_id) DO UPDATE SET
                               last_seen_us=excluded.last_seen_us,
                               message_count=conversations.message_count+excluded.message_count""",
                         (source, cid, now, now, n))
        for (hour_us, source), n in hours.items():
            conn.execute("INSERT INTO activity_hours (hour_us, source, n) VALUES (?,?,?) "
                         "ON CONFLICT(hour_us, source) DO UPDATE SET n=activity_hours.n+excluded.n",
                         (hour_us, source, n))

    def record_rejection(self, m, reason: str) -> None:
        try:
            with self._lock:
                self._conn.execute("BEGIN IMMEDIATE")
                self._conn.execute("INSERT INTO rejected (at_us, source, external_id, reason,"
                                   " snippet) VALUES (?,?,?,?,?)",
                                   (now_us(), getattr(m, "source", None),
                                    getattr(m, "external_id", None), reason[:200],
                                    (getattr(m, "text", None) or "")[:200]))
                self._conn.execute("DELETE FROM rejected WHERE id < (SELECT COALESCE(MAX(id),0)"
                                   " FROM rejected) - ?",
                                   (int(self.cfg.get("rejected_retention", 10000)),))
                self._conn.execute("COMMIT")
        except Exception:
            try:
                self._conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            log.exception("could not record rejection")

    # ------------------------------------------------------- reads / ops

    def latest_external_id(self, source):
        with self._lock:
            row = self._conn.execute("SELECT external_id FROM messages WHERE source=? "
                                     "ORDER BY CAST(external_id AS INTEGER) DESC LIMIT 1",
                                     (source,)).fetchone()
        return row[0] if row else None

    def recent_x_rows(self, limit=100):
        with self._lock:
            return self._conn.execute("SELECT id, external_id FROM messages WHERE source='x' "
                                      "ORDER BY id DESC LIMIT ?", (limit,)).fetchall()

    def refresh_metrics(self, message_rowid: int, metrics: dict) -> bool:
        """Used by the X metrics refresher: update stats + snapshot if changed."""
        mj = _metrics_json(metrics)
        with self._lock:
            row = self._conn.execute("SELECT metrics_json FROM messages WHERE id=?",
                                     (message_rowid,)).fetchone()
            if not row or row[0] == mj:
                return False
            now = now_us()
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                self._conn.execute("UPDATE messages SET metrics_json=?, last_seen_at_us=?,"
                                   " revision=revision+1 WHERE id=?", (mj, now, message_rowid))
                self._conn.execute("INSERT INTO engagement_snapshots (message_rowid,"
                                   " captured_at_us, revision, metrics_json)"
                                   " VALUES (?,?,(SELECT revision FROM messages WHERE id=?),?)",
                                   (message_rowid, now, message_rowid, mj))
                self._conn.execute("COMMIT")
                return True
            except BaseException:
                try:
                    self._conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise

    def search(self, query: str, limit: int = 50):
        with self._lock:
            return self._conn.execute(
                "SELECT m.source, m.author_username, m.posted_at_us, m.text "
                "FROM messages m JOIN messages_fts f ON f.rowid = m.id "
                "WHERE messages_fts MATCH ? ORDER BY m.ingested_at_us DESC LIMIT ?",
                (query, limit)).fetchall()

    def close(self):
        with self._lock:
            self._conn.close()