# Encrypted research snapshot — 2026-10-06

This directory stores a password-encrypted research snapshot in the standard **age** passphrase format. The archive contents, original member names and internal manifest are encrypted together.

Only the ciphertext, its SHA-256 checksum and these generic access instructions are public. Obtain the agreed passphrase separately; it is not stored in this repository.

## Restore locally

With `age` installed, download all three files in this directory and run:

```sh
shasum -a 256 -c daily-research-20261006.tar.gz.age.sha256
age --decrypt --output daily-research-20261006.tar.gz daily-research-20261006.tar.gz.age
mkdir -p restored
tar -xzf daily-research-20261006.tar.gz -C restored
```

Enter the passphrase at the hidden prompt. Open `restored/daily-research-20261006/RESTORE.txt` for the internal index and integrity instructions. Keep decrypted files outside the public Git checkout.

The snapshot preserves the framework, dated research deliverable, normalized evidence, provenance and supporting scripts. Its internal status distinguishes research specifications from completed validation; saving it does not enable trading.

## Future snapshots

Use a new dated archive, encrypt it locally with `age --passphrase`, verify a decrypt-and-checksum round trip, and publish only the encrypted archive, its ciphertext checksum and generic instructions. Never commit the passphrase, plaintext archive, internal manifest or decrypted research to public Git history.
