"""Application settings.

Single source of truth for configuration. Everything is env-driven; nothing
provider-specific is hardcoded, because Google sunsets an Ads API version each
quarter and Meta expires a Graph version every two years.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", case_sensitive=False
    )

    # --- app ---------------------------------------------------------------
    app_name: str = "Marketing Connector Platform"
    app_version: str = "0.1.0"
    environment: str = "development"
    debug: bool = True
    log_level: str = "INFO"
    api_prefix: str = "/api/v1"
    public_base_url: str = "http://localhost:8000"
    frontend_base_url: str = "http://localhost:8000"

    # --- database ----------------------------------------------------------
    database_url: str = "sqlite+aiosqlite:///./connectors.db"
    db_echo: bool = False

    # --- security ----------------------------------------------------------
    encryption_key: str = ""
    api_token: str = ""

    # --- google ------------------------------------------------------------
    google_client_id: str = ""
    google_client_secret: str = ""
    google_redirect_uri: str = "http://localhost:8000/api/v1/oauth/google/callback"
    google_ads_developer_token: str = ""
    google_ads_api_version: str = "v25"
    google_ads_login_customer_id: str = ""

    # --- meta --------------------------------------------------------------
    meta_app_id: str = ""
    meta_app_secret: str = ""
    meta_redirect_uri: str = "http://localhost:8000/api/v1/oauth/meta/callback"
    meta_api_version: str = "v26.0"

    # --- leadsquared ---------------------------------------------------------
    # Static access-key/secret-key pair (query params on every call), not
    # OAuth — see docs/coverage/LSQ_VERIFICATION_2026-09-11.md. Host is
    # region-shard-specific per account (e.g. api-in21.leadsquared.com).
    leadsquared_access_key: str = ""
    leadsquared_secret_key: str = ""
    leadsquared_host: str = "https://api.leadsquared.com"
    # Deliberately conservative and below the lower of the two documented
    # rate tiers (Pro: 5 bulk calls/5s, Super: 10/5s) — the account's actual
    # plan tier is not known (§11 of the implementation instructions: do not
    # assume it). Configurable so it can be raised once confirmed.
    #
    # Default lowered to 0.5/s after a live 429 at ~0.77/s sustained: the API budget
    # belongs to the whole account, and other systems (booking/CRM automation) draw
    # on it too. On a 429 the shared limiter halves itself for a while.
    leadsquared_rate_per_second: float = 0.5
    leadsquared_burst: int = 2
    # Calls per rolling 24h that ONE LeadSquared connection may spend. LeadSquared's documented
    # base quota is 10,000/day for the whole account, shared with the booking/CRM automation
    # that writes to it, so a backfill must never be able to spend all of it. A stream that
    # hits its share stops cleanly at a checkpoint and resumes on the next run. 0 = unlimited.
    leadsquared_daily_api_budget: int = 6000

    # --- sync engine -------------------------------------------------------
    scheduler_enabled: bool = True
    scheduler_poll_seconds: int = 30
    max_concurrent_syncs: int = 4
    sync_run_timeout_seconds: int = 10800
    # A connection lease is renewed every `sync_heartbeat_seconds`; a lease not
    # renewed for `sync_lease_seconds` is expired and may be taken over. Keep
    # lease >= ~3x heartbeat so one slow renewal does not forfeit a healthy run.
    sync_lease_seconds: int = 300
    sync_heartbeat_seconds: int = 60
    # Optional stable identity for THIS process. With it set, a restart reclaims its
    # own previous incarnation's locks immediately instead of waiting out the lease.
    # Must be unique per running process — never share one value across instances.
    worker_id: str = ""
    default_lookback_days: int = 3
    default_backfill_days: int = 90
    http_timeout_seconds: float = 120.0

    # Set by tests to keep the background scheduler out of the way.
    testing: bool = Field(default=False)

    @field_validator("google_ads_api_version")
    @classmethod
    def _ads_version_shape(cls, v: str) -> str:
        v = v.strip()
        if v and not v.startswith("v"):
            v = f"v{v}"
        return v

    @field_validator("meta_api_version")
    @classmethod
    def _meta_version_shape(cls, v: str) -> str:
        v = v.strip()
        if v and not v.startswith("v"):
            v = f"v{v}"
        return v

    @property
    def is_development(self) -> bool:
        return self.environment == "development"

    @property
    def google_oauth_configured(self) -> bool:
        return bool(self.google_client_id and self.google_client_secret)

    @property
    def meta_oauth_configured(self) -> bool:
        return bool(self.meta_app_id and self.meta_app_secret)

    @property
    def leadsquared_configured(self) -> bool:
        return bool(self.leadsquared_access_key and self.leadsquared_secret_key and self.leadsquared_host)


@lru_cache
def get_settings() -> Settings:
    return Settings()
