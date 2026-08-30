"""Gatekeeper: nothing enters the conveyor without passing here."""

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Optional

from ledger import epoch_us, now_us
from models import NormalizedMessage

ALLOWED_SOURCES = {"x", "telegram"}
MAX_TEXT_CHARS = 100_000     # X caps ~25k, Telegram 4 096 — generous ceiling
MAX_ID_CHARS = 128
MAX_FIELD_CHARS = 512
MIN_POSTED_US = epoch_us(datetime(2006, 3, 21, tzinfo=timezone.utc))  # pre-Twitter = corrupt
MAX_FUTURE_SKEW_US = 5 * 60 * 1_000_000

_BAD_CTRL = {c: None for c in range(32)}
for keep in (9, 10, 13):          # \t \n \r survive
    _BAD_CTRL.pop(keep)
_BAD_CTRL[127] = None


def _clean(s: Optional[str]) -> Optional[str]:
    return None if s is None else s.translate(_BAD_CTRL)


@dataclass
class Verdict:
    ok: bool
    reason: str = ""
    msg: Optional[NormalizedMessage] = None


def validate(m) -> Verdict:
    if not isinstance(m, NormalizedMessage):
        return Verdict(False, "not a NormalizedMessage")
    if m.source not in ALLOWED_SOURCES:
        return Verdict(False, f"bad source {m.source!r}")
    if not isinstance(m.external_id, str) or not m.external_id.strip():
        return Verdict(False, "missing external_id")
    if len(m.external_id) > MAX_ID_CHARS:
        return Verdict(False, "external_id too long")
    if not isinstance(m.posted_at, datetime):
        return Verdict(False, "posted_at missing/not a datetime")
    posted_at = m.posted_at if m.posted_at.tzinfo else m.posted_at.replace(tzinfo=timezone.utc)
    p_us = epoch_us(posted_at)
    if p_us < MIN_POSTED_US:
        return Verdict(False, f"posted_at impossibly old ({posted_at.isoformat()})")
    if p_us > now_us() + MAX_FUTURE_SKEW_US:
        return Verdict(False, f"posted_at in the future ({posted_at.isoformat()})")
    if not isinstance(m.text, str):
        return Verdict(False, "text missing")
    text = (_clean(m.text) or "").strip()
    if not text:
        return Verdict(False, "empty text")
    if len(text) > MAX_TEXT_CHARS:
        return Verdict(False, f"text too long ({len(text)} chars)")

    def fld(v, name):
        if v is None:
            return None
        if not isinstance(v, str) or len(v) > MAX_FIELD_CHARS:
            raise ValueError(name)
        return _clean(v).strip() or None

    try:
        conversation_id = fld(m.conversation_id, "conversation_id")
        author_id = fld(m.author_id, "author_id")
        author_username = fld(m.author_username, "author_username")
        reply_to = fld(m.reply_to_external_id, "reply_to_external_id")
    except ValueError as e:
        return Verdict(False, f"broken field {e}")
    if author_username:
        author_username = author_username.lstrip("@") or None

    metrics = m.metrics if isinstance(m.metrics, dict) else {}
    metrics = {str(k): v for k, v in metrics.items()}
    raw = m.raw if isinstance(m.raw, dict) else {}

    return Verdict(True, msg=replace(
        m, posted_at=posted_at, text=text, conversation_id=conversation_id,
        author_id=author_id, author_username=author_username,
        reply_to_external_id=reply_to, metrics=metrics, raw=raw))