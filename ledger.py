"""Append-only evidence ledger.

Every NEW message gets a SHA-256 fingerprint over a canonical serialization
of its identity + content + publish time (metrics deliberately excluded —
they legitimately change; their history lives in engagement_snapshots).
Fingerprints are sealed into Merkle blocks; each block hashes the previous
block's hash, forming a chain. Blocks are sealed inside the same DB
transaction as the data, so a receipt exists iff the row committed.

The chain head is also appended to a write-once anchor file after every
commit. That file (plus off-host copies of it) is what makes tampering
detectable even by someone with full admin access to the database.
"""

import hashlib
import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone

log = logging.getLogger("collector.ledger")

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
GENESIS_PREV = "0" * 64
EMPTY_ROOT = hashlib.sha256(b"").hexdigest()


def epoch_us(dt: datetime) -> int:
    return (dt.astimezone(timezone.utc) - EPOCH) // timedelta(microseconds=1)


def from_epoch_us(us: int) -> datetime:
    return EPOCH + timedelta(microseconds=us)


def now_us() -> int:
    return epoch_us(datetime.now(timezone.utc))


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


HASHED_FIELDS = ("source", "external_id", "conversation_id", "author_id",
                 "author_username", "text", "posted_at_us", "reply_to_external_id")


def canonical_payload(**f) -> str:
    """The single canonical byte representation of a message. One canonical
    form is shared by ingest and audit, so any byte that changes in the
    database changes the recomputed hash."""
    d = {k: f[k] for k in HASHED_FIELDS}
    d["posted_at_us"] = int(d["posted_at_us"])
    return json.dumps(d, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def fields_hash(**f) -> str:
    return _sha(canonical_payload(**f))


def message_hash(msg) -> str:
    return fields_hash(source=msg.source, external_id=msg.external_id,
                       conversation_id=msg.conversation_id, author_id=msg.author_id,
                       author_username=msg.author_username, text=msg.text,
                       posted_at_us=epoch_us(msg.posted_at),
                       reply_to_external_id=msg.reply_to_external_id)


def row_hash(row) -> str:
    """row = the 8 hashed columns of messages, in HASHED_FIELDS order."""
    (source, external_id, conversation_id, author_id, author_username,
     text, posted_at_us, reply_to) = row
    return fields_hash(source=source, external_id=external_id,
                       conversation_id=conversation_id, author_id=author_id,
                       author_username=author_username, text=text,
                       posted_at_us=posted_at_us, reply_to_external_id=reply_to)


# ------------------------------------------------------------------ merkle

def merkle_root(leaf_hexes) -> str:
    if not leaf_hexes:
        return EMPTY_ROOT
    level = [bytes.fromhex(h) for h in leaf_hexes]
    while len(level) > 1:
        if len(level) % 2:
            level.append(level[-1])
        level = [hashlib.sha256(level[i] + level[i + 1]).digest()
                 for i in range(0, len(level), 2)]
    return level[0].hex()


def merkle_proof(leaf_hexes, index):
    """Sibling path from leaf `index` to the root: [(sibling_hex, 'L'|'R'), …]"""
    proof, level, i = [], [bytes.fromhex(h) for h in leaf_hexes], index
    while len(level) > 1:
        if len(level) % 2:
            level.append(level[-1])
        proof.append((level[i + 1].hex(), "R") if i % 2 == 0
                     else (level[i - 1].hex(), "L"))
        level = [hashlib.sha256(level[j] + level[j + 1]).digest()
                 for j in range(0, len(level), 2)]
        i //= 2
    return proof


def verify_merkle_proof(leaf_hex, proof, root_hex) -> bool:
    h = bytes.fromhex(leaf_hex)
    for sibling_hex, side in proof:
        s = bytes.fromhex(sibling_hex)
        h = hashlib.sha256(h + s).digest() if side == "R" else hashlib.sha256(s + h).digest()
    return h.hex() == root_hex


def header_hash(seq, prev_hash, root, count, first_rowid, last_rowid, sealed_at_us):
    return _sha(f"{seq}|{prev_hash}|{root}|{count}|{first_rowid}|{last_rowid}|{sealed_at_us}")


class Ledger:
    """Seals blocks inside the caller's (Vault's) transaction; appends the
    chain head to the anchor file after the commit lands."""

    def __init__(self, anchor_path: str, max_block_size: int = 4096):
        self.anchor_path = anchor_path
        self.max_block_size = max_block_size
        self._anchor_lock = threading.Lock()
        d = os.path.dirname(anchor_path)
        if d:
            os.makedirs(d, exist_ok=True)

    # -- inside Vault's transaction ---------------------------------------

    def head(self, conn):
        row = conn.execute("SELECT seq, block_hash FROM ledger_blocks "
                           "ORDER BY seq DESC LIMIT 1").fetchone()
        return (row[0], row[1]) if row else None

    def ensure_genesis(self, conn) -> None:
        if self.head(conn) is None:
            t = now_us()
            bh = header_hash(0, GENESIS_PREV, EMPTY_ROOT, 0, 0, 0, t)
            conn.execute("INSERT INTO ledger_blocks (seq, prev_hash, merkle_root,"
                         " message_count, first_rowid, last_rowid, sealed_at_us, block_hash)"
                         " VALUES (0,?,?,0,0,0,?,?)", (GENESIS_PREV, EMPTY_ROOT, t, bh))

    def seal(self, conn, entries):
        """entries: [(message_rowid, content_hash), …] for NEW messages in
        this transaction. Chains blocks of ≤ max_block_size. Returns [(seq, hash)]."""
        sealed = []
        for start in range(0, len(entries), self.max_block_size):
            chunk = entries[start:start + self.max_block_size]
            root = merkle_root([h for _, h in chunk])
            prev_seq, prev_hash = self.head(conn)
            seq, t = prev_seq + 1, now_us()
            bh = header_hash(seq, prev_hash, root, len(chunk), chunk[0][0], chunk[-1][0], t)
            conn.execute("INSERT INTO ledger_blocks (seq, prev_hash, merkle_root,"
                         " message_count, first_rowid, last_rowid, sealed_at_us, block_hash)"
                         " VALUES (?,?,?,?,?,?,?,?)",
                         (seq, prev_hash, root, len(chunk), chunk[0][0], chunk[-1][0], t, bh))
            conn.executemany("INSERT INTO ledger_entries (block_seq, pos,"
                             " message_rowid, message_hash) VALUES (?,?,?,?)",
                             [(seq, pos, rid, h) for pos, (rid, h) in enumerate(chunk)])
            sealed.append((seq, bh))
        return sealed

    # -- after commit ------------------------------------------------------

    def anchor(self, seq: int, block_hash: str) -> None:
        """Append the chain head to the write-once anchor file (fsynced).
        Protect this file *outside* the DB too: chmod 600, `chattr +a`,
        and keep off-host copies (see runbook)."""
        with self._anchor_lock:
            new = not os.path.exists(self.anchor_path)
            with open(self.anchor_path, "a", encoding="utf-8") as f:
                f.write(f"{seq} {block_hash} {datetime.now(timezone.utc).isoformat()}\n")
                f.flush()
                os.fsync(f.fileno())
            if new:
                os.chmod(self.anchor_path, 0o600)

    def anchor_tail(self):
        try:
            with open(self.anchor_path, encoding="utf-8") as f:
                lines = [l for l in f.read().splitlines() if l.strip()]
            return (int(lines[-1].split()[0]), lines[-1].split()[1]) if lines else None
        except FileNotFoundError:
            return None

    def verify_chain(self, conn):
        """Recompute every header hash and Merkle root, check linkage from
        genesis. Returns (ok, first_problem)."""
        prev, expected = GENESIS_PREV, 0
        for seq, ph, root, count, first_rid, last_rid, t, bh in conn.execute(
                "SELECT seq, prev_hash, merkle_root, message_count, first_rowid,"
                " last_rowid, sealed_at_us, block_hash FROM ledger_blocks ORDER BY seq"):
            if seq != expected or ph != prev:
                return False, f"chain broken at block {seq}"
            if bh != header_hash(seq, ph, root, count, first_rid, last_rid, t):
                return False, f"bad header hash at block {seq}"
            leaves = [h for (h,) in conn.execute(
                "SELECT message_hash FROM ledger_entries WHERE block_seq=? ORDER BY pos",
                (seq,))]
            if len(leaves) != count or merkle_root(leaves) != root:
                return False, f"bad Merkle root at block {seq}"
            prev, expected = bh, seq + 1
        return True, None

    def reconcile_anchor(self, conn) -> None:
        """At startup: if a crash left the anchor lagging the chain, verify
        the chain and re-pin the head. If the anchor is AHEAD of the chain,
        the ledger itself has been truncated — refuse to run."""
        head = self.head(conn)
        if head is None:
            return
        ok, why = self.verify_chain(conn)
        if not ok:
            raise RuntimeError(f"ledger verification failed at startup: {why}")
        tail = self.anchor_tail()
        if tail is None:
            self.anchor(*head)
            return
        if tail[0] == head[0] and tail[1] == head[1]:
            return
        if tail[0] > head[0]:
            raise RuntimeError("anchor file is AHEAD of the ledger — possible "
                               "ledger truncation; investigate before running")
        log.warning("anchor lagged chain (crash?); re-pinning head %d", head[0])
        self.anchor(*head)