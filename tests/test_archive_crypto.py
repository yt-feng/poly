import os
import tempfile
import unittest
from pathlib import Path

from tools.archive_crypto import decrypt_bytes, encrypt_bytes


class ArchiveCryptoTests(unittest.TestCase):
    def test_round_trip_and_no_plaintext_in_envelope(self):
        plaintext = b"private strategy reasoning: do not publish"
        envelope = encrypt_bytes(plaintext, "fixture-passphrase")
        self.assertNotIn(plaintext, envelope)
        self.assertEqual(decrypt_bytes(envelope, "fixture-passphrase"), plaintext)

    def test_wrong_key_and_tampering_fail(self):
        envelope = encrypt_bytes(b"secret", "fixture-passphrase")
        with self.assertRaises(ValueError):
            decrypt_bytes(envelope, "wrong")
        tampered = bytearray(envelope)
        tampered[-3] = ord("0") if tampered[-3] != ord("0") else ord("1")
        with self.assertRaises(ValueError):
            decrypt_bytes(bytes(tampered), "fixture-passphrase")

    def test_cli_requires_runtime_environment_key(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "plain.txt"
            encrypted = root / "archive.json"
            source.write_bytes(b"offline-only")
            old = os.environ.pop("ARCHIVE_KEY", None)
            try:
                with self.assertRaises(ValueError):
                    encrypt_bytes(source.read_bytes())
            finally:
                if old is not None:
                    os.environ["ARCHIVE_KEY"] = old


if __name__ == "__main__":
    unittest.main()
