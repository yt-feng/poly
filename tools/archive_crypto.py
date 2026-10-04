"""Authenticated encryption for private research archives.

The repository is public, so only ciphertext envelopes may be committed.  The
passphrase is read from ``ARCHIVE_KEY`` at runtime and is never written to the
envelope or printed by this module.  This utility is deliberately offline and
has no trading, network, or GitHub mutation capability.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import secrets
from pathlib import Path

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt


FORMAT = "poly-research-archive/v1"
AAD = FORMAT.encode("ascii")
SALT_BYTES = 16
NONCE_BYTES = 12
KEY_BYTES = 32
SCRYPT_N = 2**14
SCRYPT_R = 8
SCRYPT_P = 1


def _passphrase(value: str | None = None) -> bytes:
    value = os.environ.get("ARCHIVE_KEY") if value is None else value
    if not value:
        raise ValueError("ARCHIVE_KEY must be set at runtime")
    return value.encode("utf-8")


def _derive_key(passphrase: bytes, salt: bytes) -> bytes:
    return Scrypt(salt=salt, length=KEY_BYTES, n=SCRYPT_N, r=SCRYPT_R, p=SCRYPT_P).derive(passphrase)


def encrypt_bytes(plaintext: bytes, passphrase: str | None = None) -> bytes:
    salt = secrets.token_bytes(SALT_BYTES)
    nonce = secrets.token_bytes(NONCE_BYTES)
    key = _derive_key(_passphrase(passphrase), salt)
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, AAD)
    envelope = {
        "format": FORMAT,
        "kdf": "scrypt",
        "kdf_params": {"n": SCRYPT_N, "r": SCRYPT_R, "p": SCRYPT_P, "length": KEY_BYTES},
        "cipher": "AES-256-GCM",
        "salt_b64": base64.b64encode(salt).decode("ascii"),
        "nonce_b64": base64.b64encode(nonce).decode("ascii"),
        "ciphertext_b64": base64.b64encode(ciphertext).decode("ascii"),
    }
    return (json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def decrypt_bytes(envelope_bytes: bytes, passphrase: str | None = None) -> bytes:
    try:
        envelope = json.loads(envelope_bytes.decode("utf-8"))
        if envelope.get("format") != FORMAT or envelope.get("kdf") != "scrypt" or envelope.get("cipher") != "AES-256-GCM":
            raise ValueError("unsupported archive envelope")
        params = envelope.get("kdf_params")
        if params != {"length": KEY_BYTES, "n": SCRYPT_N, "p": SCRYPT_P, "r": SCRYPT_R}:
            raise ValueError("unsupported KDF parameters")
        salt = base64.b64decode(envelope["salt_b64"], validate=True)
        nonce = base64.b64decode(envelope["nonce_b64"], validate=True)
        ciphertext = base64.b64decode(envelope["ciphertext_b64"], validate=True)
        if len(salt) != SALT_BYTES or len(nonce) != NONCE_BYTES:
            raise ValueError("invalid archive envelope lengths")
        key = _derive_key(_passphrase(passphrase), salt)
        return AESGCM(key).decrypt(nonce, ciphertext, AAD)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError, UnicodeDecodeError, InvalidTag) as exc:
        raise ValueError("unable to decrypt archive") from exc


def _write_atomic(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_bytes(data)
    temporary.replace(path)


def _main() -> int:
    parser = argparse.ArgumentParser(description="Encrypt or decrypt a private research archive")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("encrypt", "decrypt"):
        sub = subparsers.add_parser(command)
        sub.add_argument("input", type=Path)
        sub.add_argument("output", type=Path)
    args = parser.parse_args()
    data = args.input.read_bytes()
    output = encrypt_bytes(data) if args.command == "encrypt" else decrypt_bytes(data)
    _write_atomic(args.output, output)
    print(f"{args.command}ed archive: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
