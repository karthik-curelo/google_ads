"""Provider-agnostic OAuth 2.0 abstraction (§22).

Two providers, five connectors. Google Analytics, Search Console and Google Ads
share one `GoogleOAuthProvider`; Meta Ads and Instagram Insights share one
`MetaOAuthProvider`. Building three separate Google flows — the thing §22
explicitly forbids — would also mean three consent screens for a user who owns
all three properties.

Scopes are requested per *connector*, not per provider, so connecting only
Search Console never asks for access to a user's ad spend (§5, minimum scopes).
"""

from __future__ import annotations

import abc
import secrets
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from app.connectors.http import HttpClient, RetryPolicy


@dataclass(slots=True)
class TokenSet:
    access_token: str
    refresh_token: str | None = None
    expires_at: datetime | None = None
    scopes: list[str] = field(default_factory=list)
    token_type: str = "Bearer"

    @property
    def expired(self) -> bool:
        if self.expires_at is None:
            return False
        # 120s skew: a token that expires mid-flight fails the request it was
        # fetched for, which is a needless retry.
        return datetime.now(UTC) >= self.expires_at - timedelta(seconds=120)


@dataclass(slots=True)
class IdentityInfo:
    """Who authorised us — shown as "Connected as …" and used for dedup."""

    external_account_id: str
    email: str | None = None
    display_name: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


def expires_at_from(expires_in: Any) -> datetime | None:
    try:
        seconds = int(expires_in)
    except (TypeError, ValueError):
        return None
    if seconds <= 0:
        return None
    return datetime.now(UTC) + timedelta(seconds=seconds)


def generate_state() -> str:
    """Unguessable CSRF state. 32 bytes of urandom, url-safe."""
    return secrets.token_urlsafe(32)


class OAuthProvider(abc.ABC):
    provider: str = ""
    # Always requested — identity resolution needs them.
    base_scopes: tuple[str, ...] = ()
    # True when the provider issues refresh tokens. Meta does not: it issues
    # long-lived user tokens that must be re-authorised by a human on expiry, and
    # the platform has to model that difference rather than assume refresh works.
    supports_refresh: bool = True

    def __init__(self, *, client_id: str, client_secret: str, redirect_uri: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri

    def _client(self) -> HttpClient:
        # A token exchange is worth a couple of retries for a transient 503, but
        # not a long backoff — the user is watching a redirect complete.
        return HttpClient(
            timeout=30.0,
            retry=RetryPolicy(max_attempts=3, base_delay=0.5, max_delay=4.0, max_elapsed=20.0),
            provider=self.provider,
            max_concurrency=4,
        )

    def resolve_scopes(self, connector_scopes: Sequence[str] = ()) -> list[str]:
        """Union of base scopes and the requesting connector's scopes, order-stable."""
        seen: dict[str, None] = {}
        for scope in (*self.base_scopes, *connector_scopes):
            seen[scope] = None
        return list(seen)

    @abc.abstractmethod
    def authorization_url(self, *, state: str, scopes: Sequence[str], **kwargs: Any) -> str: ...

    @abc.abstractmethod
    async def exchange_code(self, code: str) -> TokenSet: ...

    @abc.abstractmethod
    async def refresh(self, refresh_token: str) -> TokenSet: ...

    @abc.abstractmethod
    async def fetch_identity(self, access_token: str) -> IdentityInfo: ...

    @abc.abstractmethod
    async def revoke(self, token_set: TokenSet) -> bool:
        """Best-effort revocation at the provider. False if the provider refused."""


__all__ = [
    "IdentityInfo",
    "OAuthProvider",
    "TokenSet",
    "expires_at_from",
    "generate_state",
]
