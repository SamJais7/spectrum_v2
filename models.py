from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class NormalizedMessage:
    """One message, regardless of where it came from."""
    source: str                                # "x" | "telegram"
    external_id: str                           # platform's own ID (unique per source)
    conversation_id: Optional[str]             # tweet thread / telegram chat
    author_id: Optional[str]
    author_username: Optional[str]
    text: Optional[str]
    posted_at: datetime                        # UTC, publisher's clock
    reply_to_external_id: Optional[str]        # links replies to parents
    metrics: dict = field(default_factory=dict)
    raw: dict = field(default_factory=dict)    # original payload extras (quotes, forwards…)