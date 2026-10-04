"""At-rest protection for holder credentials.

Only HMAC *hashes* of credentials are written next to lease state, and
newly issued credentials are stored authenticated-encrypted so that an
idempotent replay (even after a process restart) can hand back the exact
same credential without ever persisting it in plaintext.

The authenticated cipher uses only the Python standard library: a per
message random nonce, an HMAC-SHA256 keystream (CTR-style) and an
encrypt-then-MAC tag. Tokens are versioned ("v1") so the construction can
be replaced later without ambiguity.
"""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import os
import secrets
import threading
import time
from pathlib import Path

_TOKEN_VERSION = b"v1"
_NONCE_BYTES = 16
_MAC_BYTES = 32
# Files holding the server key are created readable/writable by the owner
# only, like an ssh private key.
_KEY_FILE_MODE = 0o600
# A reader may briefly observe a key file mid-write; retry briefly.
_KEY_READ_ATTEMPTS = 50
_KEY_READ_INTERVAL_S = 0.002
# flock locks are tied to an *open file description*, so two threads in the
# same process opening the same lock file independently would not block each
# other. A process-wide guard serializes threads; flock serializes processes.
_KEY_LOCKS_GUARD = threading.Lock()
_KEY_LOCKS: dict[str, threading.Lock] = {}


def generate_key() -> bytes:
    """Generate a fresh random 256-bit server key."""
    return secrets.token_bytes(32)


def load_or_create_key(path: str | Path) -> bytes:
    """Load the server key from ``path``, generating one on first use.

    The key file lives *outside* the SQLite database (by default a sibling
    ``<db>.key`` file): an operator inspecting database contents therefore
    sees only fingerprints and sealed tokens, never material that could be
    verified or decrypted without the separate key file.

    Creation and reads are serialized by a process-wide per-path lock (for
    threads) *and* an ``flock`` on a sibling lock file (for processes), so
    concurrent first-time openers all converge on one shared, fully written
    key and never observe a partial file.
    """
    key_path = Path(path)
    key_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = key_path.with_name(key_path.name + ".lock")
    canonical = str(key_path.resolve())
    with _KEY_LOCKS_GUARD:
        thread_lock = _KEY_LOCKS.setdefault(canonical, threading.Lock())
    with thread_lock:
        with open(lock_path, "a+b") as lock_fh:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
            if key_path.exists():
                return _read_key(key_path)
            fd = os.open(
                key_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                _KEY_FILE_MODE,
            )
            created = generate_key()
            try:
                with os.fdopen(fd, "wb") as fh:
                    fh.write(created)
                    fh.flush()
                    os.fsync(fh.fileno())
            except Exception:
                try:
                    key_path.unlink()
                except FileNotFoundError:
                    pass
                raise
            return created


def _read_key(key_path: Path) -> bytes:
    # The key is exactly 32 random bytes; it must be compared verbatim —
    # stripping would corrupt keys whose edge byte happens to be whitespace.
    last_error: ValueError | None = None
    for _ in range(_KEY_READ_ATTEMPTS):
        data = key_path.read_bytes()
        if len(data) == 32:
            return data
        last_error = ValueError(
            f"credential key file {key_path} is corrupt: expected 32 bytes"
        )
        time.sleep(_KEY_READ_INTERVAL_S)
    assert last_error is not None
    raise last_error


class CredentialCodec:
    """Hash credentials for comparison and encrypt them for replay delivery."""

    def __init__(self, key: bytes) -> None:
        if len(key) != 32:
            raise ValueError("credential key must be exactly 32 bytes")
        self._key = key

    # -- hashing -------------------------------------------------------------

    def fingerprint(self, credential: str) -> str:
        """Return the at-rest keyed fingerprint of a credential (hex)."""
        return hmac.new(
            self._key, credential.encode("utf-8"), hashlib.sha256
        ).hexdigest()

    # -- authenticated encryption -------------------------------------------

    def _keystream(self, nonce: bytes, length: int) -> bytes:
        """Deterministic HMAC-SHA256 CTR keystream for ``nonce``."""
        out = bytearray()
        counter = 0
        while len(out) < length:
            block = hmac.new(
                self._key,
                b"stream" + nonce + counter.to_bytes(8, "big"),
                hashlib.sha256,
            ).digest()
            out.extend(block)
            counter += 1
        return bytes(out[:length])

    def seal(self, plaintext: str) -> str:
        """Encrypt a credential into an opaque, tamper-evident token (hex)."""
        data = plaintext.encode("utf-8")
        nonce = secrets.token_bytes(_NONCE_BYTES)
        stream = self._keystream(nonce, len(data))
        ciphertext = bytes(a ^ b for a, b in zip(data, stream))
        body = _TOKEN_VERSION + nonce + ciphertext
        tag = hmac.new(self._key, b"mac" + body, hashlib.sha256).digest()
        return (body + tag).hex()

    def open(self, token: str) -> str:
        """Decrypt a token previously produced by :meth:`seal`.

        Raises ``ValueError`` if the token is malformed or its tag does not
        authenticate (wrong key or tampered at-rest data).
        """
        try:
            raw = bytes.fromhex(token)
        except ValueError as exc:
            raise ValueError("malformed sealed credential token") from exc
        if len(raw) < len(_TOKEN_VERSION) + _NONCE_BYTES + _MAC_BYTES:
            raise ValueError("malformed sealed credential token")
        body, tag = raw[:-_MAC_BYTES], raw[-_MAC_BYTES:]
        expected = hmac.new(self._key, b"mac" + body, hashlib.sha256).digest()
        if not hmac.compare_digest(tag, expected):
            raise ValueError("sealed credential failed authentication")
        version, rest = body[: len(_TOKEN_VERSION)], body[len(_TOKEN_VERSION):]
        if version != _TOKEN_VERSION:
            raise ValueError("unsupported sealed credential version")
        nonce, ciphertext = rest[:_NONCE_BYTES], rest[_NONCE_BYTES:]
        stream = self._keystream(nonce, len(ciphertext))
        data = bytes(a ^ b for a, b in zip(ciphertext, stream))
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("sealed credential did not decode") from exc
