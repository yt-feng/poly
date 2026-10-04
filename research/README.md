# Reproducible research records

`research/data_capture/` is reserved for the **poly** repository's public
market-data capture experiments. It must not contain trading strategy results,
account data, credentials, or BTC 5-minute strategy claims. The separate
`poly_trade` repository owns strategy research and canary evaluation.

Each experiment has a JSON metadata record and each run has a JSON manifest.
Manifests record the source commit, UTC time range, data provenance, command,
environment, and output checksums. Raw or private material belongs outside the
public tree or in an encrypted envelope created with `tools/archive_crypto.py`.

## Runtime encryption

Install the maintained PyCA dependency with `python -m pip install -r
requirements-research.txt`, then supply the passphrase only in the process
environment. The selected user passphrase is a runtime input, never a checked-in
value. The value supplied out of band by the user is provided only when the
command runs:

```bash
export ARCHIVE_KEY='<user-supplied-runtime-value>'
python tools/archive_crypto.py encrypt private-notes.json research/data_capture/private-notes.json.enc
python tools/archive_crypto.py decrypt research/data_capture/private-notes.json.enc /tmp/private-notes.json
unset ARCHIVE_KEY
```

The envelope uses scrypt and AES-256-GCM with authenticated metadata. Never
commit `ARCHIVE_KEY`, decrypted files, `.env` files, or private keys. Public
quotes alone are observations and cannot establish a fill or canary eligibility.
