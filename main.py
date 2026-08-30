import asyncio
import logging
import logging.handlers
import os
import signal
import sys
import threading
from pathlib import Path

import yaml
from dotenv import load_dotenv

from analytics_schema import ensure_schema
from collectors.telegram_collector import login_once, run_telegram
from collectors.x_collector import (request_stream_stop, run_metrics_refresher,
                                    run_poll, run_stream_blocking)
from conveyor import Conveyor
from graph.builder import run_graph_loop
from ledger import Ledger
from nlp.profiles import run_x_profile_loop
from nlp.topics import run_topic_loop
from nlp.worker import run_nlp_loop
from storage import Vault

log = logging.getLogger("collector")


def load_config(path: str = "config.yaml") -> dict:
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    load_dotenv()
    cfg.setdefault("x", {})
    cfg.setdefault("telegram", {})
    cfg.setdefault("storage", {})
    if os.getenv("X_BEARER_TOKEN"):
        cfg["x"]["bearer_token"] = os.environ["X_BEARER_TOKEN"]
    if os.getenv("TELEGRAM_API_ID"):
        cfg["telegram"]["api_id"] = os.environ["TELEGRAM_API_ID"]
    if os.getenv("TELEGRAM_API_HASH"):
        cfg["telegram"]["api_hash"] = os.environ["TELEGRAM_API_HASH"]
    return cfg


def setup_logging(cfg: dict) -> None:
    lcfg = cfg.get("logging", {})
    level = getattr(logging, str(lcfg.get("level", "INFO")).upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    root.addHandler(console)
    if lcfg.get("file"):
        Path(lcfg["file"]).parent.mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(
            lcfg["file"], maxBytes=10_000_000, backupCount=5)
        fh.setFormatter(fmt)
        root.addHandler(fh)


async def _stop_stream_when_shutdown(shutdown):
    while not shutdown.is_set():
        await asyncio.sleep(0.5)
    request_stream_stop()


async def run(cfg, vault: Vault, conveyor: Conveyor, shutdown) -> None:
    xcfg, tg_cfg = cfg["x"], cfg["telegram"]
    tasks = [asyncio.create_task(conveyor.run())]

    if xcfg.get("bearer_token") and xcfg.get("keywords"):
        if xcfg.get("mode", "stream") == "stream":
            tasks.append(asyncio.create_task(asyncio.to_thread(
                run_stream_blocking, xcfg["bearer_token"], xcfg["keywords"],
                conveyor, shutdown)))
            tasks.append(asyncio.create_task(_stop_stream_when_shutdown(shutdown)))
        else:
            tasks.append(asyncio.create_task(run_poll(
                xcfg["bearer_token"], xcfg["keywords"], conveyor, vault, shutdown,
                xcfg.get("poll_interval_seconds", 60))))
        mr = xcfg.get("metrics_refresh") or {}
        if mr.get("enabled", True):
            tasks.append(asyncio.create_task(run_metrics_refresher(
                xcfg["bearer_token"], vault, shutdown, mr.get("interval_seconds", 900))))
    else:
        log.warning("X not configured — X collection disabled")

    db = cfg["storage"]["db_path"]
    if tg_cfg.get("targets") and tg_cfg.get("api_id") and tg_cfg.get("api_hash"):
        tasks.append(asyncio.create_task(
            run_telegram(tg_cfg, conveyor, shutdown, db_path=db)))
    else:
        log.warning("Telegram not configured — collection disabled")

    acfg = cfg.get("analytics", {})
    if acfg.get("nlp", {}).get("enabled", True):
        tasks.append(asyncio.create_task(run_nlp_loop(cfg, shutdown)))
    if acfg.get("topics", {}).get("enabled", True):
        tasks.append(asyncio.create_task(run_topic_loop(cfg, shutdown)))
    if acfg.get("graph", {}).get("enabled", True):
        tasks.append(asyncio.create_task(run_graph_loop(cfg, shutdown)))
    if (acfg.get("profiles", {}).get("enabled", True) and xcfg.get("bearer_token")
            and xcfg.get("keywords")):
        tasks.append(asyncio.create_task(run_x_profile_loop(xcfg["bearer_token"], cfg, shutdown)))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for t, r in zip(tasks, results):
        if isinstance(r, Exception):
            log.error("Task %s ended with %r", t.get_name(), r)


def main():
    cfg = load_config()
    setup_logging(cfg)

    if "--login" in sys.argv:      # one-time interactive Telegram login
        asyncio.run(login_once(cfg["telegram"]))
        return

    scfg = cfg["storage"]
    ensure_schema(scfg["db_path"])               # analytics tables (additive)
    ledger = Ledger(scfg["ledger"]["anchor_path"],
                    int(scfg["ledger"].get("max_block_size", 4096)))
    vault = Vault(scfg, ledger)                  # fresh ledger seals from genesis #0
    shutdown = threading.Event()
    conveyor = Conveyor(vault, scfg, shutdown)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:          # Windows
            signal.signal(sig, lambda *_: shutdown.set())

    log.info("Collector starting (vault=%s)", scfg["db_path"])
    try:
        loop.run_until_complete(run(cfg, vault, conveyor, shutdown))
    finally:
        vault.close()
        log.info("Collector stopped cleanly")


if __name__ == "__main__":
    main()