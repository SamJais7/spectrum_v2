#!/usr/bin/env python3
"""Evidence audit CLI.

  python audit.py verify                                    full integrity check
  python audit.py prove --source telegram --id "-10012345/678"
  python audit.py timeline --from "2025-01-07 14:00" --to "2025-01-07 14:15"
  python audit.py reanchor                                  re-pin head after verified crash-lag
"""
import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone

import yaml

from ledger import (Ledger, epoch_us, from_epoch_us, merkle_proof,
                    row_hash, verify_merkle_proof)


def _load():
    with open("config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    s = cfg["storage"]
    return s["db_path"], Ledger(s["ledger"]["anchor_path"])


def _connect(db_path, readonly=True):
    if readonly:
        try:
            return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
        except sqlite3.Error:
            pass
    return sqlite3.connect(db_path, timeout=30)


def _iso(s):
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def cmd_verify(args, conn, ledger):
    problems, checked = [], 0

    ok, why = ledger.verify_chain(conn)                       # 1. the chain itself
    if not ok:
        problems.append(f"LEDGER: {why}")

    sql = ("SELECT e.message_rowid, e.message_hash, m.source, m.external_id,"
           " m.conversation_id, m.author_id, m.author_username, m.text,"
           " m.posted_at_us, m.reply_to_external_id"
           " FROM ledger_entries e LEFT JOIN messages m ON m.id = e.message_rowid")
    if args.sample:                                           # 2. DB vs receipts
        sql += f" ORDER BY RANDOM() LIMIT {int(args.sample)}"
    for row in conn.execute(sql):
        checked += 1
        (rid, ehash, src, eid, conv, aid, auser, text, p_us, reply) = row
        if src is None:
            problems.append(f"receipt for rowid {rid}: message row MISSING (deleted?)")
            continue
        if row_hash((src, eid, conv, aid, auser, text, p_us, reply)) != ehash:
            problems.append(f"rowid {rid} ({src}/{eid}): content does not match its "
                            "receipt — TAMPERED")

    orphan = conn.execute(                                    # 3. every row has a receipt
        "SELECT COUNT(*) FROM messages m LEFT JOIN ledger_entries e "
        "ON e.message_rowid = m.id WHERE e.block_seq IS NULL").fetchone()[0]
    if orphan:
        problems.append(f"{orphan} messages have no ledger receipt")

    head, tail = ledger.head(conn), ledger.anchor_tail()      # 4. anchor file
    if head and tail:
        if tail[0] > head[0]:
            problems.append("anchor file is AHEAD of the ledger — possible truncation")
        elif tail[1] != head[1] and tail[0] == head[0]:
            problems.append("anchor head hash does not match chain head")
        elif tail[0] < head[0]:
            print(f"note: anchor lags chain by {head[0] - tail[0]} blocks "
                  "(crash window? run `reanchor` after verify passes)")

    n_msgs = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    n_blocks = conn.execute("SELECT COUNT(*) FROM ledger_blocks").fetchone()[0]
    print(f"messages={n_msgs} ledger_blocks={n_blocks} receipts_crosschecked={checked}")
    if problems:
        print(f"\nFAILED — {len(problems)} problem(s):")
        for p in problems[:50]:
            print("  ✗", p)
        sys.exit(1)
    print("VERIFIED: ledger chain intact, every checked message matches its receipt,"
          " anchor consistent.")


def cmd_prove(args, conn, ledger):
    row = conn.execute("SELECT id, source, external_id, conversation_id, author_id,"
                       " author_username, text, posted_at_us, reply_to_external_id"
                       " FROM messages WHERE source=? AND external_id=?",
                       (args.source, args.id)).fetchone()
    if not row:
        sys.exit(f"no such message: {args.source}/{args.id}")
    (rid, src, eid, conv, aid, auser, text, p_us, reply) = row
    live = row_hash((src, eid, conv, aid, auser, text, p_us, reply))

    ent = conn.execute("SELECT block_seq, pos, message_hash FROM ledger_entries"
                       " WHERE message_rowid=?", (rid,)).fetchone()
    if not ent:
        sys.exit("message exists but has NO ledger receipt — cannot prove")
    bseq, pos, ehash = ent
    blk = conn.execute("SELECT prev_hash, merkle_root, message_count, first_rowid,"
                       " last_rowid, sealed_at_us, block_hash FROM ledger_blocks"
                       " WHERE seq=?", (bseq,)).fetchone()
    leaves = [h for (h,) in conn.execute("SELECT message_hash FROM ledger_entries"
                                         " WHERE block_seq=? ORDER BY pos", (bseq,))]
    proof = merkle_proof(leaves, pos)
    chain_ok, why = ledger.verify_chain(conn)

    cert = {
        "message": {"source": src, "external_id": eid, "conversation_id": conv,
                    "author_id": aid, "author_username": auser, "text": text,
                    "posted_at": from_epoch_us(p_us).isoformat(),
                    "reply_to_external_id": reply},
        "recomputed_content_hash": live,
        "ledger_entry": {"block_seq": bseq, "pos": pos, "message_hash": ehash},
        "merkle_proof": proof,
        "block_header": {"seq": bseq, "prev_hash": blk[0], "merkle_root": blk[1],
                         "message_count": blk[2],
                         "sealed_at": from_epoch_us(blk[5]).isoformat(),
                         "block_hash": blk[6]},
        "chain_to_genesis": [{"seq": s, "block_hash": b} for s, b in conn.execute(
            "SELECT seq, block_hash FROM ledger_blocks WHERE seq<=? ORDER BY seq", (bseq,))],
        "checks": {
            "recomputed_hash_matches_receipt": live == ehash,
            "merkle_proof_valid": verify_merkle_proof(live, proof, blk[1]),
            "chain_intact": chain_ok,
        },
    }
    out = json.dumps(cert, indent=2, ensure_ascii=False)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(out)
        print(f"certificate written to {args.out}")
    else:
        print(out)
    print("\nPROOF " + ("VALID ✓" if all(cert["checks"].values())
                        else "INVALID ✗ — " + str(cert["checks"])))


def cmd_timeline(args, conn, ledger):
    rows = conn.execute(
        "SELECT source, author_username, datetime(posted_at_us/1000000,'unixepoch'),"
        " substr(text,1,80) FROM messages WHERE posted_at_us>=? AND posted_at_us<?"
        " ORDER BY posted_at_us LIMIT ?",
        (epoch_us(_iso(args.fm)), epoch_us(_iso(args.to)), args.limit)).fetchall()
    print(f"{len(rows)} message(s) between {args.fm} and {args.to} (UTC):")
    for src, user, at, text in rows:
        print(f"  {at}  {src:8s} @{user or '?'}  {text}")
    rollup = conn.execute(
        "SELECT datetime(hour_us/1000000,'unixepoch'), source, n FROM activity_hours"
        " WHERE hour_us>=? AND hour_us<? ORDER BY hour_us",
        (epoch_us(_iso(args.fm)), epoch_us(_iso(args.to)))).fetchall()
    for at, src, n in rollup:
        print(f"  [hour rollup] {at}  {src}: {n}")


def cmd_reanchor(args, conn, ledger):
    ok, why = ledger.verify_chain(conn)
    if not ok:
        sys.exit(f"refusing to re-anchor a broken chain: {why}")
    ledger.anchor(*ledger.head(conn))
    print("chain head re-pinned:", ledger.head(conn))


def main():
    db, ledger = _load()
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    v = sub.add_parser("verify"); v.add_argument("--sample", type=int, default=0,
                                                 help="verify a random N receipts instead of all")
    pr = sub.add_parser("prove"); pr.add_argument("--source", required=True)
    pr.add_argument("--id", required=True); pr.add_argument("--out")
    t = sub.add_parser("timeline"); t.add_argument("--from", dest="fm", required=True)
    t.add_argument("--to", required=True); t.add_argument("--limit", type=int, default=100)
    sub.add_parser("reanchor")
    args = p.parse_args()

    conn = _connect(db, readonly=args.cmd != "reanchor")
    {"verify": cmd_verify, "prove": cmd_prove,
     "timeline": cmd_timeline, "reanchor": cmd_reanchor}[args.cmd](args, conn, ledger)


if __name__ == "__main__":
    main()