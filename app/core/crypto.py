"""Credential encryption at rest.

OAuth refresh tokens are long-lived bearer credentials for a user's ad spend and
analytics data, so they never touch the database in plaintext. Fernet gives
authenticated symmetric encryption (AES-128-CBC + HMAC-SHA256) from a dependency
we already need for nothing else — no KMS to stand up.

Key rotation: ENCRYPTION_KEY accepts a comma-separated list, newest first.
MultiFernet encrypts with the first key and decrypts with any, so rotating is
"prepend the new key, redeploy" and rows re-encrypt as they are next written.
"""

from __future__ import annotations

import base64
import hashlib
import logging
from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken, MultiFernet

from app.core.config import get_settings

logger = logging.getLogger(__name__)


class CredentialCryptoError(RuntimeError):
    """Raised when a stored credential cannot be decrypted."""


def _derive_dev_key(seed: str) -> str:
    """Deterministic throwaway key so `git clone && uvicorn` works with no setup.

    Development only. Anything other than ENVIRONMENT=development refuses to
    start without a real ENCRYPTION_KEY, because a key derived from a constant
    in a public repo is not a secret.
    """
    digest = hashlib.sha256(f"dev-only-do-not-use-in-production::{seed}".encode()).digest()
    return base64.urlsafe_b64encode(digest).decode()


@lru_cache
def _cipher() -> MultiFernet:
    settings = get_settings()
    raw_keys = [k.strip() for k in settings.encryption_key.split(",") if k.strip()]

    if not raw_keys:
        if not settings.is_development:
            raise RuntimeError(
                "ENCRYPTION_KEY is not set. Generate one with:\n"
                '  python -c "from cryptography.fernet import Fernet;'
                'print(Fernet.generate_key().decode())"'
            )
        logger.warning(
            "ENCRYPTION_KEY unset — using an insecure key derived for development. "
            "Set a real key before any non-development use."
        )
        raw_keys = [_derive_dev_key(settings.app_name)]

    keys = []
    for key in raw_keys:
        try:
            keys.append(Fernet(key.encode()))
        except (ValueError, TypeError) as exc:
            raise RuntimeError(
                "ENCRYPTION_KEY contains an invalid Fernet key (expected 32 url-safe "
                "base64-encoded bytes). Generate one with Fernet.generate_key()."
            ) from exc
    return MultiFernet(keys)


def encrypt(plaintext: str) -> str:
    return _cipher().encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str) -> str:
    try:
        return _cipher().decrypt(ciphertext.encode()).decode()
    except InvalidToken as exc:
        # Almost always a rotated-away or mismatched ENCRYPTION_KEY. Say so
        # without echoing the ciphertext into the logs.
        raise CredentialCryptoError(
            "Stored credential could not be decrypted — ENCRYPTION_KEY may have "
            "changed. Reconnect the affected integration to re-authorise."
        ) from exc


def encrypt_optional(plaintext: str | None) -> str | None:
    return encrypt(plaintext) if plaintext else None


def decrypt_optional(ciphertext: str | None) -> str | None:
    return decrypt(ciphertext) if ciphertext else None
