"""Append-only gzip segments and durable GitHub Release publication.

A ready file is closed, checksummed and safe to upload. No market-data bytes
are committed to the code branch. GitHub Releases are a bootstrap destination;
large/all-symbol deployments should mirror these immutable files to object storage.
"""
from __future__ import annotations
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
from datetime import datetime, timezone


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def atomic_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding='utf-8')
    temp.replace(path)


def gh(*args: str) -> str:
    return subprocess.run(['gh', *args], check=True, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout


def ensure_release(tag: str) -> None:
    try:
        gh('release', 'view', tag)
    except subprocess.CalledProcessError:
        # Any authorization failure also fails creation; never mark an upload done.
        gh('release', 'create', tag, '--target', 'main', '--title', tag,
           '--notes', 'Append-only public market-data archive; see CAPTURE_V2.md.',
           '--latest=false')


def publish(tag: str, paths: list[Path], *, replace: bool = False) -> None:
    if not paths:
        return
    ensure_release(tag)
    for path in paths:
        args = ['release', 'upload', tag, str(path)]
        if replace:
            args.append('--clobber')
        gh(*args)


class Archive:
    def __init__(self, root: Path, segment_seconds: int = 900,
                 max_bytes: int = 64 * 1024 * 1024):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)
        self.segment_seconds = segment_seconds
        self.max_bytes = max_bytes
        self.opened = {}
        self.sequence = 0
        self.records = []

    def write(self, kind: str, row: dict) -> None:
        now = time.time()
        day = datetime.fromtimestamp(now, timezone.utc).strftime('%Y-%m-%d')
        current = self.opened.get(kind)
        if current and (now - current['start'] >= self.segment_seconds
                        or current['bytes'] >= self.max_bytes or current['day'] != day):
            self.close_kind(kind)
            current = None
        if current is None:
            self.sequence += 1
            path = self.root / f'{kind}-{day}-{self.sequence:06d}.jsonl.gz'
            current = dict(path=path, handle=gzip.open(str(path) + '.part', 'wb', compresslevel=1),
                           start=now, day=day, bytes=0, rows=0)
            self.opened[kind] = current
        data = (json.dumps(row, separators=(',', ':'), ensure_ascii=False) + '\n').encode()
        current['handle'].write(data)
        current['bytes'] += len(data)
        current['rows'] += 1

    def close_kind(self, kind: str) -> None:
        item = self.opened.pop(kind, None)
        if not item:
            return
        item['handle'].close()
        path = item['path']
        Path(str(path) + '.part').replace(path)
        digest = sha256(path)
        path.with_suffix(path.suffix + '.sha256').write_text(f'{digest}  {path.name}\n')
        self.records.append(dict(file=path.name, sha256=digest, rows=item['rows'],
                                 bytes=path.stat().st_size, kind=kind))
        atomic_json(self.root / 'manifest.json', {'schema_version': 2, 'files': self.records})

    def close(self) -> None:
        for kind in list(self.opened):
            self.close_kind(kind)


def upload_ready(root: Path, tag: str) -> None:
    """Runs off the sampling thread. Files remain local after durable upload."""
    uploaded_path = root / '.uploaded.json'
    uploaded = set(json.loads(uploaded_path.read_text())) if uploaded_path.exists() else set()
    for path in sorted(root.glob('*.jsonl.gz')):
        checksum = path.with_suffix(path.suffix + '.sha256')
        if path.name in uploaded or not checksum.exists():
            continue
        # A retry may follow an ambiguous network result. Immutable local bytes
        # and a run-specific tag make replacing the same asset idempotent.
        publish(tag, [path, checksum], replace=True)
        uploaded.add(path.name)
        atomic_json(uploaded_path, sorted(uploaded))
    manifest = root / 'manifest.json'
    if manifest.exists():
        publish(tag, [manifest], replace=True)
