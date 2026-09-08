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

    # --- sync engine -------------------------------------------------------
    scheduler_enabled: bool = True
    scheduler_poll_seconds: int = 30
    max_concurrent_syncs: int = 4
    sync_run_timeout_seconds: int = 10800
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


@lru_cache
def get_settings() -> Settings:
    return Settings()
