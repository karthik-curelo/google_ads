"""Structured logging with secret masking.

§20 of the spec is non-negotiable: access tokens, refresh tokens, client secrets
and authorization codes must never reach the logs. Relying on every call site to
remember that is how secrets leak, so masking is enforced centrally by a
logging filter that rewrites records on their way out. Provider error payloads
routinely echo the request back, which is exactly how a token ends up in an
exception message.
"""

from __future__ import annotations

import logging
import re

from app.core.config import get_settings

# Patterns are deliberately broad — a false positive costs a masked log line,
# a false negative leaks a credential.
_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    # key="value" / key: value / key=value in JSON, query strings and repr output
    (
        re.compile(
            r"(?i)\b(access_token|refresh_token|client_secret|app_secret|id_token|"
            r"authorization_code|developer[_-]?token|api_token|encryption_key|password|"
            r"code_verifier|client_assertion|access[_-]?key|secret[_-]?key)\b(\"?\s*[:=]\s*\"?)([^\"'\s,&}\]]+)"
        ),
        r"\1\2***REDACTED***",
    ),
    # Authorization: Bearer <jwt-ish>
    (re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/\-]{8,}=*"), r"\1***REDACTED***"),
    # Google OAuth token shapes leak via URLs and error bodies.
    (re.compile(r"\bya29\.[A-Za-z0-9._\-]+"), "ya29.***REDACTED***"),
    (re.compile(r"\b1//[A-Za-z0-9._\-]{10,}"), "1//***REDACTED***"),
    # Meta long-lived user tokens.
    (re.compile(r"\bEAA[A-Za-z0-9]{20,}"), "EAA***REDACTED***"),
)


def mask_secrets(text: str) -> str:
    """Redact anything credential-shaped. Safe to call on arbitrary strings."""
    for pattern, replacement in _PATTERNS:
        text = pattern.sub(replacement, text)
    return text


class SecretMaskingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            # Render args in now so masking also covers %-style interpolation.
            if record.args:
                record.msg = record.getMessage()
                record.args = ()
            if isinstance(record.msg, str):
                record.msg = mask_secrets(record.msg)
            if record.exc_text:
                record.exc_text = mask_secrets(record.exc_text)
        except Exception:  # pragma: no cover - logging must never raise
            pass
        return True


def setup_logging() -> None:
    settings = get_settings()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    handler.addFilter(SecretMaskingFilter())

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.getLevelName(settings.log_level))

    # httpx logs full request URLs at INFO, which for OAuth token exchange means
    # the authorization code. The filter would catch it, but not logging it at
    # all is cheaper and less fragile.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not any(isinstance(f, SecretMaskingFilter) for f in logger.filters):
        logger.addFilter(SecretMaskingFilter())
    return logger
