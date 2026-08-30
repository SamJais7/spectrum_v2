import asyncio
import logging
from typing import List

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError

from models import NormalizedMessage

log = logging.getLogger("collector.telegram")


async def _make_client(cfg) -> TelegramClient:
    return TelegramClient(
        cfg.get("session_name", "collector"),
        int(cfg["api_id"]),
        cfg["api_hash"],
        connection_retries=None,    # keep retrying forever
        retry_delay=5,
        auto_reconnect=True,
        flood_sleep_threshold=120,  # auto-sleep through short "slow down" warnings
    )


async def login_once(cfg) -> None:
    """Run once interactively (`python main.py --login`) to create the session file."""
    client = await _make_client(cfg)
    await client.start()
    me = await client.get_me()
    print(f"Logged in as {getattr(me, 'first_name', '')} (@{getattr(me, 'username', None)}) "
          f"— session file saved. You can now run headless.")
    await client.disconnect()


async def _wait_for(shutdown):
    while not shutdown.is_set():
        await asyncio.sleep(1)


async def _sleep(shutdown, seconds: float):
    for _ in range(int(seconds) + 1):
        if shutdown.is_set():
            return
        await asyncio.sleep(1)


async def _handle_event(sink, event) -> None:
    try:
        msg = event.message
        if msg.is_private:          # belt & braces: DMs are never stored
            return
        text = (msg.message or "").strip()
        if not text:                # text messages only
            return

        chat = await event.get_chat()
        sender = await event.get_sender()

        reply_to = None
        if msg.reply_to and msg.reply_to.reply_to_msg_id:
            reply_to = f"{chat.id}/{msg.reply_to.reply_to_msg_id}"

        views = getattr(msg, "views", None)
        sender_name = " ".join(filter(None, [getattr(sender, "first_name", None),
                                             getattr(sender, "last_name", None)]))
        raw = {
            "chat": {"id": chat.id, "title": getattr(chat, "title", None),
                     "username": getattr(chat, "username", None)},
            "sender": {"id": getattr(sender, "id", None),
                       "username": getattr(sender, "username", None),
                       "name": sender_name or None},
            "mentions": [m for m in (msg.get_entities_text("mention") or [])][:20],
        }
        if msg.fwd_from:
            raw["forward"] = {
                "user_id": getattr(msg.fwd_from.from_id, "user_id", None)
                           if msg.fwd_from.from_id else None,
                "username": getattr(msg.fwd_from, "from_name", None),
            }

        norm = NormalizedMessage(
            source="telegram",
            external_id=f"{chat.id}/{msg.id}",     # IDs are per-chat → combine
            conversation_id=str(chat.id),
            author_id=str(getattr(sender, "id", "")) or None,
            author_username=getattr(sender, "username", None),
            text=text,
            posted_at=msg.date,                    # tz-aware UTC
            reply_to_external_id=reply_to,
            metrics={"views": views} if views else {},
            raw=raw,
        )
        sink.submit(norm)
    except Exception:
        log.exception("failed to process Telegram message")


def _make_handler(sink):
    async def handler(event):
        await _handle_event(sink, event)
    return handler


async def _profile_loop(client, db_path, refresh_hours, max_per, shutdown):
    """Fetch PUBLIC Telegram bios (about text) for known authors, re-infer demographics."""
    from analytics_schema import analytics_connect   # chunk 3 (lazy import)
    from ledger import now_us                        # chunk 2 (lazy import)
    from nlp.profiles import upsert_profile, refresh_demographics
    from telethon.tl.functions.users import GetFullUserRequest
    refresh_us = refresh_hours * 3_600_000_000
    while not shutdown.is_set():
        try:
            conn = analytics_connect(db_path)
            try:
                rows = conn.execute(
                    "SELECT a.author_id, a.username FROM authors a "
                    "LEFT JOIN author_profiles p ON p.source=a.source AND p.author_id=a.author_id "
                    "WHERE a.source='telegram' AND (p.author_id IS NULL OR p.fetched_at_us<?) "
                    "ORDER BY a.last_seen_us DESC LIMIT ?",
                    (now_us() - refresh_us, max_per)).fetchall()
                for aid, username in rows:
                    if shutdown.is_set():
                        return
                    try:
                        res = await client(GetFullUserRequest(id=int(aid)))
                        u = res.users[0] if res.users else None
                        bio = (res.full_user.about or "")[:500]
                        changed = upsert_profile(conn, "telegram", str(aid),
                                                 getattr(u, "username", None) or username,
                                                 bio, None, False, {})
                        if changed:
                            refresh_demographics(conn, "telegram", str(aid), None)
                    except Exception:
                        pass                      # deleted accounts, privacy, floods
                    await asyncio.sleep(1.2)
            finally:
                conn.close()
        except Exception:
            log.exception("telegram profile cycle failed")
        for _ in range(300):
            if shutdown.is_set():
                return
            await asyncio.sleep(12)


async def run_telegram(cfg, sink, shutdown, db_path: str = None) -> None:
    targets: List[str] = cfg.get("targets") or []
    backoff = 5
    while not shutdown.is_set():
        client = None
        try:
            client = await _make_client(cfg)
            await client.start()      # uses saved session after first --login

            chat_ids = []
            for t in targets:
                try:
                    ent = await client.get_entity(t)
                    chat_ids.append(ent.id)
                    log.info("Telegram target: %s -> %s (id=%s)", t,
                             getattr(ent, "title", None) or getattr(ent, "username", None),
                             ent.id)
                except Exception as e:
                    log.error("Cannot resolve Telegram target %r: %s", t, e)
            if not chat_ids:
                raise RuntimeError("no valid Telegram targets configured")

            if cfg.get("catch_up", True):
                try:
                    await client.catch_up()   # fetch what was posted while offline
                except Exception:
                    log.exception("catch_up failed; continuing with live updates")

            client.add_event_handler(_make_handler(sink),
                                      events.NewMessage(chats=chat_ids))
            log.info("Telegram listening on %d chats", len(chat_ids))

            if db_path:
                pcfg = cfg.get("profiles", {})
                asyncio.create_task(_profile_loop(
                    client, db_path, pcfg.get("refresh_hours", 12),
                    pcfg.get("max_per_cycle", 200), shutdown))

            runner = asyncio.create_task(client.run_until_disconnected())
            stopper = asyncio.create_task(_wait_for(shutdown))
            await asyncio.wait({runner, stopper}, return_when=asyncio.FIRST_COMPLETED)
            for t in (runner, stopper):
                if not t.done():
                    t.cancel()

            if shutdown.is_set():
                break
            log.warning("Telegram disconnected; reconnecting in %ss", backoff)
            await _sleep(shutdown, backoff)
        except FloodWaitError as e:
            log.warning("Telegram flood wait: sleeping %ss", e.seconds)
            await _sleep(shutdown, e.seconds)
        except Exception:
            log.exception("Telegram collector error; retrying in %ss", backoff)
            await _sleep(shutdown, backoff)
            backoff = min(backoff * 2, 300)
        finally:
            if client is not None and client.is_connected():
                await client.disconnect()
    log.info("Telegram collector stopped")