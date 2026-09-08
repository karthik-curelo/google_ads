import pytest

from app.connectors import errors as E
from app.core.crypto import (
    CredentialCryptoError,
    _cipher,
    decrypt,
    decrypt_optional,
    encrypt,
    encrypt_optional,
)
from app.core.logging import mask_secrets


def test_encrypt_roundtrip_and_optional():
    token = "1//refresh-token-value"
    assert decrypt(encrypt(token)) == token
    assert encrypt_optional(None) is None
    assert decrypt_optional(None) is None


def test_decrypt_rejects_foreign_ciphertext():
    from cryptography.fernet import Fernet

    other = Fernet(Fernet.generate_key()).encrypt(b"x").decode()
    with pytest.raises(CredentialCryptoError):
        decrypt(other)


def test_key_rotation_reads_old_writes_new(monkeypatch):
    from cryptography.fernet import Fernet

    old_key = Fernet.generate_key().decode()
    old_cipher = Fernet(old_key)
    legacy = old_cipher.encrypt(b"secret").decode()

    new_key = Fernet.generate_key().decode()
    monkeypatch.setenv("ENCRYPTION_KEY", f"{new_key},{old_key}")
    _cipher.cache_clear()
    from app.core.config import get_settings

    get_settings.cache_clear()
    try:
        assert decrypt(legacy) == "secret"  # old key still decrypts
        assert decrypt(encrypt("new")) == "new"  # new key roundtrips
    finally:
        _cipher.cache_clear()
        get_settings.cache_clear()


def test_mask_secrets_redacts_tokens():
    assert "REDACTED" in mask_secrets('access_token="ya29.abcdef123456"')
    assert "ya29.abcdef" not in mask_secrets("token ya29.abcdefghijklmnop")
    assert "REDACTED" in mask_secrets("Authorization: Bearer abcdefghijklmnop")
    assert "EAA" in mask_secrets("EAAB" + "x" * 40) and "REDACTED" in mask_secrets("EAAB" + "x" * 40)


def test_error_flag_semantics():
    assert E.authentication_error("x").retryable is False
    assert E.authentication_error("x").recoverable is True
    assert E.rate_limit_error("x").retryable is True
    assert E.provider_unavailable("x").retryable is True
    wrapped = E.wrap_unexpected(ValueError("boom"))
    assert wrapped.code == E.ErrorCode.UNKNOWN_ERROR
    assert "boom" in wrapped.message
    already = E.permission_error("nope")
    assert E.wrap_unexpected(already) is already
