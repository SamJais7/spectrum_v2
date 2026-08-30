import asyncio
import logging
import time
from typing import Callable, List, Optional

import tweepy

from models import NormalizedMessage

log = logging.getLogger("collector.x")

TWEET_FIELDS = ["id", "text", "author_id", "created_at",
                "conversation_id", "public_metrics", "referenced_tweets"]
USER_FIELDS = ["id", "username"]
EXPANSIONS = ["author_id"]

_current_stream: Optional["XStream"] = None


def request_stream_stop():
    if _current_stream is not None:
        try:
            _current_stream.disconnect()
        except Exception:
            pass


# ------------------------------------------------------------ normalization

def _normalize(tweet, username: Optional[str]) -> NormalizedMessage:
    reply_to = quoted = None
    for ref in (tweet.referenced_tweets or []):
        if ref.type == "replied_to":
            reply_to = str(ref.id)
        elif ref.type == "quoted":
            quoted = str(ref.id)
    return NormalizedMessage(
        source="x",
        external_id=str(tweet.id),
        conversation_id=str(getattr(tweet, "conversation_id", None) or tweet.id),
        author_id=str(tweet.author_id) if tweet.author_id else None,
        author_username=username,
        text=tweet.text,
        posted_at=tweet.created_at,               # tz-aware UTC from the API
        reply_to_external_id=reply_to,
        metrics=dict(tweet.public_metrics or {}),  # likes, reposts, replies, quotes
        raw={"quoted_id": quoted} if quoted else {},
    )


def _persist(sink, msg) -> None:
    """Push onto the conveyor: validates, queues or spills — never blocks."""
    sink.submit(msg)


# ------------------------------------------------------- mode 1: live stream

class XStream(tweepy.StreamingClient):
    def __init__(self, bearer_token: str, on_message: Callable):
        super().__init__(
            bearer_token,
            wait_on_rate_limit=True,   # politely pause when X says slow down
            tweet_fields=TWEET_FIELDS,
            expansions=EXPANSIONS,
            user_fields=USER_FIELDS,
        )
        self._on_message = on_message

    def on_response(self, response):
        tweet = response.data
        users = (response.includes or {}).get("users", [])
        username = users[0].username if users else None
        msg = _normalize(tweet, username)
        tags = [r.tag for r in (response.matching_rules or []) if r.tag]
        msg.raw = {**msg.raw, "matched_keywords": tags}   # keep quoted_id too
        self._on_message(msg)

    def on_connect(self):
        log.info("X stream connected")

    def on_connection_closed(self):
        log.warning("X stream closed; reconnecting")

    def on_errors(self, errors):
        log.warning("X stream errors: %s", errors)
        return True  # keep the stream alive

    def on_exception(self, exception):
        log.error("X stream exception: %r", exception)


def _sync_rules(stream: XStream, keywords: List[str]):
    """Make stream rules match config (idempotent across restarts)."""
    existing = stream.get_rules().data or []
    have = {r.value: r.id for r in existing}
    wanted = set(keywords)
    stale = [rid for value, rid in have.items() if value not in wanted]
    if stale:
        stream.delete_rules(stale)
    missing = [kw for kw in keywords if kw not in have]
    if missing:
        stream.add_rules([tweepy.StreamRule(value=kw, tag=kw[:128]) for kw in missing])


def _sleep(shutdown, seconds: float):
    end = time.monotonic() + seconds
    while not shutdown.is_set() and time.monotonic() < end:
        time.sleep(min(1.0, max(0.0, end - time.monotonic())))


def run_stream_blocking(bearer_token: str, keywords: List[str], sink, shutdown) -> None:
    """Runs in a worker thread; reconnects forever until shutdown."""
    global _current_stream
    backoff = 5
    log.info("X stream starting for %d rules", len(keywords))
    while not shutdown.is_set():
        try:
            stream = XStream(bearer_token, on_message=lambda m: _persist(sink, m))
            _current_stream = stream
            _sync_rules(stream, keywords)
            stream.filter(threaded=False, stall_warnings=True)  # blocks
            _sleep(shutdown, 5)
        except tweepy.TooManyRequests:
            log.warning("X rate limit (429): pausing 5 minutes")
            _sleep(shutdown, 300)
        except Exception:
            log.exception("X stream error; retrying in %ss", backoff)
            _sleep(shutdown, backoff)
            backoff = min(backoff * 2, 300)
        finally:
            _current_stream = None
    log.info("X stream stopped")


# ------------------------------------------------- mode 2: polling fallback

async def run_poll(bearer_token: str, keywords: List[str], sink, vault,
                   shutdown, interval: int = 60) -> None:
    client = tweepy.Client(bearer_token, wait_on_rate_limit=True)
    query = " OR ".join(keywords)
    last = vault.latest_external_id("x")
    since_id = int(last) if last else None
    log.info("X poller started (query=%r since_id=%s)", query, since_id)
    while not shutdown.is_set():
        try:
            resp = client.search_recent_tweets(
                query=query, since_id=since_id, max_results=100,
                tweet_fields=TWEET_FIELDS, expansions=EXPANSIONS,
                user_fields=USER_FIELDS,
            )
            if resp.data:
                users = {u.id: u.username for u in (resp.includes or {}).get("users", [])}
                for tweet in sorted(resp.data, key=lambda t: t.id):
                    _persist(sink, _normalize(tweet, users.get(tweet.author_id)))
                    if since_id is None or tweet.id > since_id:
                        since_id = tweet.id
        except tweepy.Unauthorized:
            log.error("X API unauthorized — check X_BEARER_TOKEN")
            return
        except Exception:
            log.exception("X poll cycle failed")
        for _ in range(max(1, int(interval))):
            if shutdown.is_set():
                return
            await asyncio.sleep(1)


# ------------------------------------------- interaction-count refresher

async def run_metrics_refresher(bearer_token: str, vault, shutdown,
                                interval: int = 900, batch: int = 100) -> None:
    """Re-fetch interaction counts for recent tweets; update + snapshot on change."""
    client = tweepy.Client(bearer_token, wait_on_rate_limit=True)
    log.info("X metrics refresher started (every %ss)", interval)
    while not shutdown.is_set():
        try:
            rows = vault.recent_x_rows(limit=batch)
            if rows:
                resp = client.get_tweets(ids=[ext for _, ext in rows],
                                         tweet_fields=["public_metrics"])
                by_id = {t.id: t for t in (resp.data or [])}
                n = sum(1 for rowid, ext in rows
                        if ext.isdigit() and by_id.get(int(ext))
                        and vault.refresh_metrics(rowid,
                                                  dict(by_id[int(ext)].public_metrics or {})))
                if n:
                    log.info("Refreshed metrics for %d tweets", n)
        except Exception:
            log.exception("metrics refresh failed")
        for _ in range(max(30, int(interval))):
            if shutdown.is_set():
                return
            await asyncio.sleep(1)