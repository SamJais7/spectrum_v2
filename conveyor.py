"""The conveyor belt: a bounded in-memory waiting room in front of the vault
with a disk-backed overflow lane, so a viral flood stacks up neatly instead of
crashing anything. The vault writer is idempotent (upsert by source+external_id),
so replaying any line twice is always harmless."""

import asyncio
import json
import logging
import os
import queue
import threading
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import validation
from models import NormalizedMessage

log = logging.getLogger("collector.conveyor")

_FIELDS = ("source", "external_id", "conversation_id", "author_id", "author_username",
           "text", "posted_at", "reply_to_external_id", "metrics", "raw")


class Conveyor:
    def __init__(self, vault, cfg: dict, shutdown):
        self.vault, self.shutdown = vault, shutdown
        c = cfg.get("conveyor", {})
        self.capacity = int(c.get("capacity", 100_000))
        self.max_batch = int(c.get("max_batch", 2_000))
        self.max_wait_ms = int(c.get("max_wait_ms", 200))
        self.overflow_path = c.get("overflow_path", "data/overflow.jsonl")
        self._q = queue.Queue(maxsize=self.capacity)
        self._ofile_lock = threading.Lock()
        self._spilled = self._rejected = self._written = 0
        Path(self.overflow_path).parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------ producers

    def submit(self, msg) -> None:
        """Thread-safe, never raises, never blocks: validate → queue, or
        overflow lane, or reject bin."""
        v = validation.validate(msg)
        if not v.ok:
            self._rejected += 1
            self.vault.record_rejection(msg, v.reason)
            log.debug("rejected (%s): %s", v.reason, getattr(msg, "external_id", "?"))
            return
        try:
            self._q.put_nowait(v.msg)
        except queue.Full:
            self._spill([v.msg])

    def _spill(self, items) -> None:
        """Disk overflow; also doubles as the crash-recovery spool if a DB
        write ever fails."""
        try:
            with self._ofile_lock, open(self.overflow_path, "a", encoding="utf-8") as f:
                for m in items:
                    d = asdict(m)
                    d["posted_at"] = m.posted_at.isoformat()
                    f.write(json.dumps(d, ensure_ascii=False, default=str) + "\n")
                f.flush()
                os.fsync(f.fileno())
            self._spilled += len(items)
        except Exception:
            log.exception("OVERFLOW SPILL FAILED — %d items at risk", len(items))

    # ------------------------------------------------------------- consumer

    def _next_batch(self):
        """Blocking drain of up to max_batch items (runs in a worker thread)."""
        try:
            batch = [self._q.get(timeout=0.5)]
        except queue.Empty:
            return []
        deadline = time.monotonic() + self.max_wait_ms / 1000
        while len(batch) < self.max_batch:
            try:
                batch.append(self._q.get_nowait())
            except queue.Empty:
                if time.monotonic() >= deadline:
                    break
                time.sleep(0.002)
        return batch

    async def run(self) -> None:
        log.info("conveyor up (capacity=%d batch<=%d wait<=%dms)",
                 self.capacity, self.max_batch, self.max_wait_ms)
        last_stats, drain_deadline = time.monotonic(), None
        while True:
            if self.shutdown.is_set():
                if drain_deadline is None:
                    drain_deadline = time.monotonic() + 30     # graceful final drain
                    log.info("shutdown: draining conveyor (≤30s)")
                if self._q.empty() or time.monotonic() > drain_deadline:
                    break
            batch = await asyncio.to_thread(self._next_batch)
            if batch:
                await asyncio.to_thread(self._write, batch)
            else:                                              # idle → drain overflow
                await asyncio.to_thread(self._replay_overflow, self.max_batch)
            if time.monotonic() - last_stats > 60:
                last_stats = time.monotonic()
                log.info("conveyor: queued=%d spilled_total=%d rejected_total=%d "
                         "written_total=%d overflow_file=%dB",
                         self._q.qsize(), self._spilled, self._rejected,
                         self._written, self._overflow_size())
        end = time.monotonic() + 30                            # final overflow sweep
        while time.monotonic() < end and self._overflow_size() > 0:
            await asyncio.to_thread(self._replay_overflow, self.max_batch)

    def _write(self, batch) -> None:
        try:
            res = self.vault.write_batch(batch)
            self._written += len(batch)
            log.info("vault: +%d new, %d metric updates, %d unchanged, block #%d sealed",
                     res.inserted, res.updated, res.unchanged, res.last_block_seq)
        except Exception:
            log.exception("vault write failed; spilling %d items to overflow", len(batch))
            self._spill(batch)
            time.sleep(1)

    # -------------------------------------------------------- overflow lane

    def _overflow_size(self) -> int:
        try:
            return os.path.getsize(self.overflow_path)
        except OSError:
            return 0

    def _replay_overflow(self, limit) -> None:
        if self._overflow_size() == 0:
            return
        with self._ofile_lock:
            try:
                with open(self.overflow_path, encoding="utf-8") as f:
                    lines = f.readlines()
                if not lines:
                    return
                parsed, drop = [], set()
                for i, line in enumerate(lines[:limit]):
                    try:
                        d = json.loads(line)
                        d["posted_at"] = datetime.fromisoformat(d["posted_at"])
                        parsed.append(NormalizedMessage(**{k: d.get(k) for k in _FIELDS}))
                        drop.add(i)
                    except Exception:
                        log.warning("quarantining corrupt overflow line: %.120s", line)
                if parsed:
                    try:
                        res = self.vault.write_batch(parsed)
                        self._written += res.inserted
                        log.info("overflow replay: %d items restored", res.inserted)
                    except Exception:
                        log.exception("overflow replay write failed; will retry")
                        return                                  # file left untouched
                if len(drop) < len(lines[:limit]):              # park corrupt lines
                    with open(self.overflow_path + ".corrupt", "a", encoding="utf-8") as f:
                        f.writelines(l for i, l in enumerate(lines[:limit]) if i not in drop)
                self._rewrite([l for i, l in enumerate(lines) if i not in drop])
            except Exception:
                log.exception("overflow replay failed; will retry")

    def _rewrite(self, remaining_lines) -> None:
        tmp = self.overflow_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.writelines(remaining_lines)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.overflow_path)