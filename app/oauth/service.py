"""OAuth orchestration against the database.

Holds the three things a connector must never know about: where tokens are
stored, how CSRF state is validated, and when a token needs refreshing.

Token refresh is serialised per identity by an in-process lock. Without it, a
connection syncing four streams concurrently fires four simultaneous refreshes;
Google honours the first and may invalidate the rest, which presents as random
`invalid_grant` failures under load.

ponytail: the lock is per-process. A second worker process could still race.
Swap it for `SELECT ... FOR UPDATE` on the identity row if this is ever deployed
with more than one worker.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors import errors as E
from app.connectors.registry import load_connectors
from app.core.config import Settings, get_settings
from app.core.crypto import decrypt_optional, encrypt_optional
from app.core.database import SessionLocal
from app.core.logging import get_logger
from app.models import (
    IDENTITY_ACTIVE,
    IDENTITY_NEEDS_REAUTH,
    IDENTITY_REVOKED,
    OAuthIdentity,
    OAuthState,
)
from app.oauth.base import IdentityInfo, OAuthProvider, TokenSet, generate_state
from app.oauth.google import GoogleOAuthProvider
from app.oauth.meta import MetaOAuthProvider

logger = get_logger(__name__)

STATE_TTL_SECONDS = 600  # 10 minutes is ample for a consent screen.

_refresh_locks: dict[int, asyncio.Lock] = {}


def _lock_for(identity_id: int) -> asyncio.Lock:
    lock = _refresh_locks.get(identity_id)
    if lock is None:
        lock = _refresh_locks[identity_id] = asyncio.Lock()
    return lock


def get_oauth_provider(provider: str, settings: Settings | None = None) -> OAuthProvider:
    settings = settings or get_settings()
    if provider == "google":
        if not settings.google_oauth_configured:
            raise E.invalid_configuration(
                "Google OAuth is not configured.",
                provider="google",
                user_action="Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET, then restart.",
            )
        return GoogleOAuthProvider(
            client_id=settings.google_client_id,
            client_secret=settings.google_client_secret,
            redirect_uri=settings.google_redirect_uri,
        )
    if provider == "meta":
        if not settings.meta_oauth_configured:
            raise E.invalid_configuration(
                "Meta OAuth is not configured.",
                provider="meta",
                user_action="Set META_APP_ID and META_APP_SECRET, then restart.",
            )
        return MetaOAuthProvider(
            client_id=settings.meta_app_id,
            client_secret=settings.meta_app_secret,
            redirect_uri=settings.meta_redirect_uri,
            api_version=settings.meta_api_version,
        )
    raise E.invalid_configuration(f"Unknown OAuth provider {provider!r}.")


# ---------------------------------------------------------------------------
# Authorisation start
# ---------------------------------------------------------------------------


async def begin_authorization(
    session: AsyncSession,
    *,
    organization_id: int,
    connector_id: str,
    redirect_after: str | None = None,
    identity_id: int | None = None,
    login_hint: str | None = None,
) -> tuple[str, str]:
    """Create CSRF state and return (authorization_url, state)."""
    entry = load_connectors().get(connector_id)
    connector_cls = entry.connector_class
    provider_name = connector_cls.provider
    provider = get_oauth_provider(provider_name)

    scopes = provider.resolve_scopes(connector_cls.required_scopes)
    state = generate_state()

    session.add(
        OAuthState(
            state=state,
            organization_id=organization_id,
            provider=provider_name,
            connector_id=connector_id,
            scopes=scopes,
            redirect_after=redirect_after,
            identity_id=identity_id,
            expires_at=datetime.now(UTC) + timedelta(seconds=STATE_TTL_SECONDS),
        )
    )
    await session.flush()

    url = provider.authorization_url(state=state, scopes=connector_cls.required_scopes, login_hint=login_hint)
    return url, state


# ---------------------------------------------------------------------------
# Callback
# ---------------------------------------------------------------------------


async def consume_state(session: AsyncSession, state: str) -> OAuthState:
    """Validate and single-use the CSRF state.

    Rejecting reuse matters: an attacker who captures a callback URL must not be
    able to replay it, and a user double-clicking the consent button must not
    create two identities.
    """
    if not state:
        raise E.invalid_configuration(
            "The OAuth callback was missing its state parameter.",
            user_action="Start the connection flow again.",
        )
    row = (await session.execute(select(OAuthState).where(OAuthState.state == state))).scalar_one_or_none()
    if row is None:
        raise E.authentication_error(
            "This authorisation request is not recognised (possible CSRF, or the server restarted).",
            user_action="Start the connection flow again.",
        )
    if row.consumed_at is not None:
        raise E.authentication_error(
            "This authorisation request has already been used.",
            user_action="Start the connection flow again.",
        )
    expires_at = row.expires_at
    if expires_at.tzinfo is None:  # SQLite round-trips naive datetimes
        expires_at = expires_at.replace(tzinfo=UTC)
    if expires_at < datetime.now(UTC):
        raise E.authentication_error(
            "This authorisation request expired before it was completed.",
            user_action="Start the connection flow again.",
        )
    row.consumed_at = datetime.now(UTC)
    await session.flush()
    return row


async def complete_authorization(
    session: AsyncSession, *, state: str, code: str
) -> tuple[OAuthIdentity, OAuthState]:
    """Exchange the code, resolve the identity, and persist encrypted tokens."""
    state_row = await consume_state(session, state)
    provider = get_oauth_provider(state_row.provider)

    token_set = await provider.exchange_code(code)
    info = await provider.fetch_identity(token_set.access_token)

    identity = await upsert_identity(
        session,
        organization_id=state_row.organization_id,
        provider=state_row.provider,
        info=info,
        token_set=token_set,
        expected_identity_id=state_row.identity_id,
    )
    return identity, state_row


async def upsert_identity(
    session: AsyncSession,
    *,
    organization_id: int,
    provider: str,
    info: IdentityInfo,
    token_set: TokenSet,
    expected_identity_id: int | None = None,
) -> OAuthIdentity:
    existing = (
        await session.execute(
            select(OAuthIdentity).where(
                OAuthIdentity.organization_id == organization_id,
                OAuthIdentity.provider == provider,
                OAuthIdentity.external_account_id == info.external_account_id,
            )
        )
    ).scalar_one_or_none()

    if expected_identity_id is not None and existing is not None and existing.id != expected_identity_id:
        # Reconnect flow signed in as a different account than the one being
        # repaired. Silently repointing the connection would attach a stranger's
        # data to it.
        raise E.invalid_configuration(
            "You signed in with a different account than the one being reconnected.",
            provider=provider,
            user_action="Reconnect again and choose the originally connected account.",
        )

    scopes = sorted(set(token_set.scopes or []))
    if existing is None:
        identity = OAuthIdentity(
            organization_id=organization_id,
            provider=provider,
            external_account_id=info.external_account_id,
            email=info.email,
            display_name=info.display_name,
            scopes=scopes,
        )
        session.add(identity)
    else:
        identity = existing
        identity.email = info.email or identity.email
        identity.display_name = info.display_name or identity.display_name
        # Incremental authorisation accumulates scopes — a token granted for
        # Search Console must not appear to have lost the Analytics scope.
        identity.scopes = sorted(set(identity.scopes or []) | set(scopes))

    identity.access_token_encrypted = encrypt_optional(token_set.access_token)
    if token_set.refresh_token:
        identity.refresh_token_encrypted = encrypt_optional(token_set.refresh_token)
    identity.access_token_expires_at = token_set.expires_at
    identity.status = IDENTITY_ACTIVE
    identity.status_detail = None
    identity.revoked_at = None
    await session.flush()
    return identity


async def disconnect_identity(session: AsyncSession, identity: OAuthIdentity) -> bool:
    """Revoke at the provider, then mark locally revoked. Local always succeeds."""
    revoked = False
    try:
        provider = get_oauth_provider(identity.provider)
        token_set = TokenSet(
            access_token=decrypt_optional(identity.access_token_encrypted) or "",
            refresh_token=decrypt_optional(identity.refresh_token_encrypted),
        )
        revoked = await provider.revoke(token_set)
    except Exception as exc:  # noqa: BLE001 - disconnect must never be blocked
        logger.warning("Provider revocation failed during disconnect: %s", exc)

    identity.status = IDENTITY_REVOKED
    identity.revoked_at = datetime.now(UTC)
    # Destroy the credentials regardless of whether the provider acknowledged.
    identity.access_token_encrypted = None
    identity.refresh_token_encrypted = None
    identity.access_token_expires_at = None
    await session.flush()
    return revoked


# ---------------------------------------------------------------------------
# Token provider
# ---------------------------------------------------------------------------


class DatabaseTokenProvider:
    """`TokenProvider` backed by an `oauth_identities` row.

    Owns its own sessions so a long sync can refresh a token without borrowing
    the request session or holding a transaction open across HTTP calls.
    """

    def __init__(self, identity_id: int, *, settings: Settings | None = None):
        self.identity_id = identity_id
        self.settings = settings or get_settings()
        self._token: TokenSet | None = None
        self._provider_name: str | None = None
        self._scopes: list[str] = []
        self._label: str | None = None

    @property
    def scopes(self) -> Sequence[str]:
        return self._scopes

    @property
    def account_label(self) -> str | None:
        return self._label

    async def _load(self) -> TokenSet:
        async with SessionLocal() as session:
            identity = await session.get(OAuthIdentity, self.identity_id)
            if identity is None:
                raise E.invalid_configuration("The connected account no longer exists.")
            if identity.status == IDENTITY_REVOKED:
                raise E.authentication_error(
                    "This integration was disconnected.",
                    provider=identity.provider,
                    user_action="Reconnect the integration to resume syncing.",
                )
            self._provider_name = identity.provider
            self._scopes = list(identity.scopes or [])
            self._label = identity.email or identity.display_name
            expires_at = identity.access_token_expires_at
            if expires_at is not None and expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=UTC)
            return TokenSet(
                access_token=decrypt_optional(identity.access_token_encrypted) or "",
                refresh_token=decrypt_optional(identity.refresh_token_encrypted),
                expires_at=expires_at,
                scopes=self._scopes,
            )

    async def access_token(self) -> str:
        if self._token is None:
            self._token = await self._load()
        if not self._token.expired and self._token.access_token:
            return self._token.access_token

        async with _lock_for(self.identity_id):
            # Re-read under the lock: another coroutine may have just refreshed.
            self._token = await self._load()
            if not self._token.expired and self._token.access_token:
                return self._token.access_token
            return await self._refresh_locked()

    async def invalidate(self) -> None:
        """Force a refresh on the next call (provider rejected a live token)."""
        if self._token is not None:
            self._token.expires_at = datetime.now(UTC) - timedelta(seconds=1)

    async def _refresh_locked(self) -> str:
        assert self._token is not None
        provider_name = self._provider_name or ""
        provider = get_oauth_provider(provider_name, self.settings)

        if not provider.supports_refresh or not self._token.refresh_token:
            await self._mark_needs_reauth(
                "The provider's authorisation expired and cannot be refreshed automatically."
            )
            raise E.authentication_error(
                f"The {provider_name.title()} authorisation has expired.",
                provider=provider_name,
                user_action="Click Reconnect and sign in again to restore access.",
            )

        try:
            refreshed = await provider.refresh(self._token.refresh_token)
        except E.ConnectorError as exc:
            if exc.code == E.ErrorCode.AUTHENTICATION_ERROR:
                await self._mark_needs_reauth(exc.message)
            raise

        async with SessionLocal() as session:
            identity = await session.get(OAuthIdentity, self.identity_id)
            if identity is not None:
                identity.access_token_encrypted = encrypt_optional(refreshed.access_token)
                if refreshed.refresh_token:
                    identity.refresh_token_encrypted = encrypt_optional(refreshed.refresh_token)
                identity.access_token_expires_at = refreshed.expires_at
                identity.status = IDENTITY_ACTIVE
                identity.status_detail = None
                if refreshed.scopes:
                    identity.scopes = sorted(set(identity.scopes or []) | set(refreshed.scopes))
            await session.commit()

        self._token = refreshed
        logger.info("Refreshed %s access token for identity %s", provider_name, self.identity_id)
        return refreshed.access_token

    async def _mark_needs_reauth(self, detail: str) -> None:
        async with SessionLocal() as session:
            identity = await session.get(OAuthIdentity, self.identity_id)
            if identity is not None:
                identity.status = IDENTITY_NEEDS_REAUTH
                identity.status_detail = detail
            await session.commit()


def token_hash(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode()).hexdigest()


async def purge_expired_states(session: AsyncSession, *, older_than_hours: int = 24) -> int:
    """Housekeeping so `oauth_states` does not grow without bound."""
    from sqlalchemy import delete

    cutoff = datetime.now(UTC) - timedelta(hours=older_than_hours)
    result = await session.execute(delete(OAuthState).where(OAuthState.created_at < cutoff))
    return result.rowcount or 0


__all__: list[str] = [
    "DatabaseTokenProvider",
    "begin_authorization",
    "complete_authorization",
    "consume_state",
    "disconnect_identity",
    "get_oauth_provider",
    "purge_expired_states",
    "token_hash",
    "upsert_identity",
]


# Keep a reference so `Any` import is used in annotations of dynamic payloads.
_: Any = None
