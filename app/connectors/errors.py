"""Structured connector errors (§29).

Every failure that crosses a connector boundary becomes one of these. Two
booleans carry the semantics that actually drive behaviour:

  retryable    can the *same* request succeed later without human action?
               (429, 503, socket timeout — yes. 403 insufficient scope — no.)
  recoverable  can the user fix it themselves? (reconnect, grant a permission,
               correct a setting — yes. Provider outage — no.)

This is Airbyte's `config_error` vs `system_error` split with the ambiguity
removed. It matters because retrying a permanent auth failure forever is how a
sync engine burns quota and buries the real problem — §14 explicitly forbids it.

`user_action` is the string the UI shows a non-technical user. `technical_details`
never reaches them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class ErrorCode:
    AUTHENTICATION_ERROR = "AUTHENTICATION_ERROR"
    PERMISSION_ERROR = "PERMISSION_ERROR"
    RATE_LIMIT_ERROR = "RATE_LIMIT_ERROR"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    NETWORK_ERROR = "NETWORK_ERROR"
    INVALID_CONFIGURATION = "INVALID_CONFIGURATION"
    RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND"
    API_SCHEMA_ERROR = "API_SCHEMA_ERROR"
    DATA_VALIDATION_ERROR = "DATA_VALIDATION_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    PROVIDER_UNAVAILABLE = "PROVIDER_UNAVAILABLE"
    NOT_SUPPORTED = "NOT_SUPPORTED"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


@dataclass(slots=True)
class ConnectorError(Exception):
    code: str = ErrorCode.UNKNOWN_ERROR
    message: str = "An unexpected error occurred."
    provider: str | None = None
    connector_id: str | None = None
    stream: str | None = None
    retryable: bool = False
    recoverable: bool = False
    user_action: str | None = None
    http_status: int | None = None
    retry_after_seconds: float | None = None
    technical_details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"[{self.code}] {self.message}"

    def as_user_dict(self) -> dict[str, Any]:
        """Safe to return over the API — no stack traces, no provider payloads."""
        return {
            "code": self.code,
            "message": self.message,
            "provider": self.provider,
            "connector": self.connector_id,
            "stream": self.stream,
            "retryable": self.retryable,
            "recoverable": self.recoverable,
            "user_action": self.user_action,
        }

    def as_log_dict(self) -> dict[str, Any]:
        return {
            **self.as_user_dict(),
            "http_status": self.http_status,
            "technical_details": self.technical_details,
        }


# --- constructors ----------------------------------------------------------
# Named helpers rather than subclasses: the behaviour lives in the flags, and a
# class hierarchy per code would be ten classes with no bodies.


def _err(
    code: str,
    message: str,
    *,
    retryable: bool,
    recoverable: bool,
    default_user_action: str | None = None,
    **kw: Any,
) -> ConnectorError:
    """Build a ConnectorError, letting the caller override user_action / flags via kw."""
    kw.setdefault("user_action", default_user_action)
    kw.setdefault("retryable", retryable)
    kw.setdefault("recoverable", recoverable)
    return ConnectorError(code=code, message=message, **kw)


def authentication_error(message: str, **kw: Any) -> ConnectorError:
    return _err(
        ErrorCode.AUTHENTICATION_ERROR,
        message,
        retryable=False,
        recoverable=True,
        default_user_action="Reconnect this integration to re-authorise access.",
        **kw,
    )


def permission_error(message: str, **kw: Any) -> ConnectorError:
    return _err(
        ErrorCode.PERMISSION_ERROR,
        message,
        retryable=False,
        recoverable=True,
        default_user_action=(
            "The connected account lacks access to this resource. Grant it access "
            "with the provider, or reconnect using an account that has it."
        ),
        **kw,
    )


def rate_limit_error(message: str, retry_after_seconds: float | None = None, **kw: Any) -> ConnectorError:
    return _err(
        ErrorCode.RATE_LIMIT_ERROR,
        message,
        retryable=True,
        recoverable=False,
        retry_after_seconds=retry_after_seconds,
        default_user_action="The provider is rate limiting requests. The sync will retry automatically.",
        **kw,
    )


def quota_exceeded(message: str, **kw: Any) -> ConnectorError:
    return _err(
        ErrorCode.QUOTA_EXCEEDED,
        message,
        # Daily quotas do reset, but not within a run's retry budget.
        retryable=False,
        recoverable=True,
        default_user_action=(
            "The provider's API quota for this project is exhausted. It resets on the "
            "provider's schedule — reduce sync frequency or request more quota."
        ),
        **kw,
    )


def network_error(message: str, **kw: Any) -> ConnectorError:
    return _err(
        ErrorCode.NETWORK_ERROR,
        message,
        retryable=True,
        recoverable=False,
        default_user_action="A network problem interrupted the sync. It will retry automatically.",
        **kw,
    )


def invalid_configuration(message: str, user_action: str | None = None, **kw: Any) -> ConnectorError:
    return _err(
        ErrorCode.INVALID_CONFIGURATION,
        message,
        retryable=False,
        recoverable=True,
        user_action=user_action or "Correct this connection's configuration and try again.",
        **kw,
    )


def resource_not_found(message: str, **kw: Any) -> ConnectorError:
    return _err(
        ErrorCode.RESOURCE_NOT_FOUND,
        message,
        retryable=False,
        recoverable=True,
        default_user_action=(
            "The selected account or property no longer exists, or is no longer "
            "visible to the connected account. Pick a different one."
        ),
        **kw,
    )


def api_schema_error(message: str, **kw: Any) -> ConnectorError:
    return _err(
        ErrorCode.API_SCHEMA_ERROR,
        message,
        retryable=False,
        recoverable=False,
        default_user_action="The provider returned data in an unexpected shape. This needs a connector update.",
        **kw,
    )


def data_validation_error(message: str, **kw: Any) -> ConnectorError:
    return _err(
        ErrorCode.DATA_VALIDATION_ERROR,
        message,
        retryable=False,
        recoverable=False,
        default_user_action="Some records could not be processed. See the skipped-record log.",
        **kw,
    )


def database_error(message: str, **kw: Any) -> ConnectorError:
    return _err(
        ErrorCode.DATABASE_ERROR,
        message,
        retryable=True,
        recoverable=False,
        default_user_action="Writing to the database failed. The sync will retry.",
        **kw,
    )


def provider_unavailable(message: str, **kw: Any) -> ConnectorError:
    return _err(
        ErrorCode.PROVIDER_UNAVAILABLE,
        message,
        retryable=True,
        recoverable=False,
        default_user_action="The provider's API is temporarily unavailable. The sync will retry.",
        **kw,
    )


def not_supported(message: str, user_action: str | None = None, **kw: Any) -> ConnectorError:
    """For capabilities the provider genuinely does not offer for this asset.

    Used instead of failing opaquely — e.g. Instagram insights on a personal
    account, which no endpoint will ever return (§10).
    """
    return _err(
        ErrorCode.NOT_SUPPORTED,
        message,
        retryable=False,
        recoverable=False,
        user_action=user_action or "This capability is not available for the selected account type.",
        **kw,
    )


def timeout_error(message: str, **kw: Any) -> ConnectorError:
    return _err(
        ErrorCode.TIMEOUT,
        message,
        retryable=True,
        recoverable=False,
        default_user_action="The request took too long. The sync will retry.",
        **kw,
    )


def unknown_error(message: str, **kw: Any) -> ConnectorError:
    return _err(
        ErrorCode.UNKNOWN_ERROR,
        message,
        retryable=False,
        recoverable=False,
        default_user_action="An unexpected error occurred. Check the run log for details.",
        **kw,
    )


def wrap_unexpected(exc: BaseException, **kw: Any) -> ConnectorError:
    """Last-resort conversion so nothing escapes the framework untyped."""
    if isinstance(exc, ConnectorError):
        return exc
    return ConnectorError(
        code=ErrorCode.UNKNOWN_ERROR,
        message=f"{type(exc).__name__}: {exc}",
        retryable=False,
        recoverable=False,
        user_action="An unexpected error occurred. Check the run log for details.",
        technical_details={"exception_type": type(exc).__name__},
        **kw,
    )
