from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Optional


@dataclass(frozen=True)
class ProxyConfig:
    """Per-account proxy configuration.

    Keep it minimal for MVP. Contributors can extend it to support auth, rotation, etc.
    """

    url: str  # e.g. http://user:pass@host:port or socks5://host:port

    def __repr__(self) -> str:
        return "ProxyConfig(url='[REDACTED]')"

    def __str__(self) -> str:
        return self.__repr__()


@dataclass(frozen=True)
class AccountAuth:
    """LinkedIn auth material.

    MVP: accept raw cookie values.

    - li_at is usually the primary session cookie.
    - JSESSIONID is sometimes required for CSRF headers.

    IMPORTANT: treat these as secrets; never log.
    """

    li_at: str
    jsessionid: Optional[str] = None

    def __repr__(self) -> str:
        return "AccountAuth(li_at='[REDACTED]', jsessionid='[REDACTED]')"

    def __str__(self) -> str:
        return self.__repr__()


@dataclass(frozen=True)
class LinkedInRuntimeHints:
    """Optional live browser metadata for LinkedIn's GraphQL contract.

    These values are opportunistically captured from a real browser session and
    let the provider track LinkedIn contract drift without requiring a code
    change every time query IDs rotate.
    """

    x_li_track: Optional[str] = None
    csrf_token: Optional[str] = None
    conversations_query_id: Optional[str] = None
    messages_query_id: Optional[str] = None

    def merged_with(self, newer: Optional["LinkedInRuntimeHints"]) -> "LinkedInRuntimeHints":
        if newer is None:
            return self
        return LinkedInRuntimeHints(
            x_li_track=newer.x_li_track or self.x_li_track,
            csrf_token=newer.csrf_token or self.csrf_token,
            conversations_query_id=(
                newer.conversations_query_id or self.conversations_query_id
            ),
            messages_query_id=newer.messages_query_id or self.messages_query_id,
        )

    def is_empty(self) -> bool:
        return not any((
            self.x_li_track,
            self.csrf_token,
            self.conversations_query_id,
            self.messages_query_id,
        ))

    def __repr__(self) -> str:
        return (
            "LinkedInRuntimeHints("
            f"x_li_track={'[REDACTED]' if self.x_li_track else None}, "
            f"csrf_token={'[REDACTED]' if self.csrf_token else None}, "
            f"conversations_query_id={self.conversations_query_id!r}, "
            f"messages_query_id={self.messages_query_id!r})"
        )

    def __str__(self) -> str:
        return self.__repr__()


@dataclass(frozen=True)
class Account:
    id: int
    label: str
    created_at: datetime


@dataclass(frozen=True)
class Thread:
    id: int
    account_id: int
    platform_thread_id: str
    title: Optional[str]
    created_at: datetime


@dataclass(frozen=True)
class Message:
    id: int
    account_id: int
    thread_id: int
    platform_message_id: str
    direction: Literal["in", "out"]
    sender: Optional[str]
    text: Optional[str]
    sent_at: datetime
    raw: Optional[dict[str, Any]] = None
