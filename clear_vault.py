#!/usr/bin/env python3
"""clear_vault.py — wipe the collected archive for a clean-slate test run.

Deletes:  vault DB (+ WAL/SHM sidecars), ledger anchor, overflow lane.
Keeps:    collector.session (Telegram login — no re-login needed), config,
          code, learned models, logs.

Why the anchor must go too: the ledger refuses to start if the anchor file
is AHEAD of the chain (tamper-truncation guard). A wiped DB + old anchor
trips that guard on purpose — so both reset together, and the next boot
seals a fresh ledger from genesis block #0.

Usage:
    python clear_vault.py            # with confirmation prompt
    python clear_vault.py --yes      # no prompt
    python clear_vault.py --backup   # copy the DB to data/backups/ first
"""

import argparse
import os
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml


def _load_paths():
    with open("config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    s = cfg["storage"]
    return s["db_path"], s["ledger"]["anchor_path"], \
        s.get("conveyor", {}).get("overflow_path", "data/overflow.jsonl")


def _collector_running(db_path: str) -> bool:
    """Best-effort write-lock probe: BEGIN IMMEDIATE needs the same lock
    the vault's batch writer holds. Two attempts — the lock is only held
    ~200ms per batch, so a single miss could be a false alarm."""
    if not os.path.exists(db_path):
        return False
    for _ in range(2):
        try:
            conn = sqlite3.connect(db_path, timeout=1)
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("ROLLBACK")
            conn.close()
            return False
        except sqlite3.OperationalError:
            time.sleep(1)
    return True


def _backup(db_path: str) -> None:
    bdir = Path("data/backups")
    bdir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    for src, ext in ((db_path, ".db"), (db_path + "-wal", ".db-wal")):
        if os.path.exists(src):
            shutil.copy2(src, bdir / f"vault-{stamp}{ext}")
    print(f"  backup saved: data/backups/vault-{stamp}.db")


def _rm(path: str) -> bool:
    try:
        os.remove(path)
        print(f"  deleted   {path}")
        return True
    except FileNotFoundError:
        return False
    except OSError as e:
        print(f"  !! FAILED {path}: {e}")
        return False


def main():
    ap = argparse.ArgumentParser(description="Reset the vault to a fresh state.")
    ap.add_argument("--yes", action="store_true", help="skip confirmation")
    ap.add_argument("--backup", action="store_true", help="copy DB before wiping")
    args = ap.parse_args()

    db_path, anchor_path, overflow_path = _load_paths()

    print("clear_vault — resetting the archive")
    print(f"  db:      {db_path}")
    print(f"  anchor:  {anchor_path}")
    print(f"  overflow:{overflow_path}")
    print("  kept:    collector.session (Telegram login), config, models, logs\n")

    if _collector_running(db_path):
        print("ABORT: the collector appears to be running (database is locked).")
        print("       Stop it first (Ctrl+C in its terminal), then re-run this script.")
        raise SystemExit(1)

    n_rows = 0
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path, timeout=2)
            n_rows = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            conn.close()
        except sqlite3.Error:
            n_rows = -1
    if n_rows > 0:
        print(f"  this will permanently delete {n_rows} collected messages "
              f"and their ledger receipts\n")
    if not args.yes and n_rows != 0:
        if input("type 'yes' to wipe: ").strip().lower() != "yes":
            print("aborted — nothing deleted.")
            return

    if args.backup and os.path.exists(db_path):
        _backup(db_path)

    print("\nwiping:")
    if n_rows == 0 and not os.path.exists(anchor_path):
        print("  (already clean — nothing to delete)")
    _rm(db_path)
    _rm(db_path + "-wal")
    _rm(db_path + "-shm")
    _rm(anchor_path)               # MUST go with the DB — truncation guard
    _rm(overflow_path)
    _rm(overflow_path + ".corrupt")
    _rm(overflow_path + ".tmp")

    print("\ndone. Next start: fresh ledger from block #0, backfill re-pulls")
    print("the newest messages per chat (watermarks lived in the DB and reset too).")
    print("Run: python main.py")


if __name__ == "__main__":
    main()