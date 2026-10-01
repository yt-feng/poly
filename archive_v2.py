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
import re
import subprocess
import time
import threading
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


def safe_diagnostic(value, limit=2000) -> str:
    """Bound diagnostics, never subprocess stdout, and remove common credentials."""
    text = value.decode('utf-8', errors='replace') if isinstance(value, bytes) else ('' if value is None else str(value))
    for name in ('GH_TOKEN', 'GITHUB_TOKEN', 'GH_ENTERPRISE_TOKEN', 'GITHUB_ENTERPRISE_TOKEN'):
        secret = os.environ.get(name)
        if secret:
            text = text.replace(secret, '[REDACTED]')
    text = re.sub(r'\b(?:gh[pousr]_[A-Za-z0-9_]+|github_pat_[A-Za-z0-9_]+)\b', '[REDACTED]', text)
    text = re.sub(r'(?im)\b(authorization|proxy-authorization|cookie|set-cookie)\s*:[^\r\n]*', r'\1: [REDACTED]', text)
    text = re.sub(r'(?i)([?&](?:access_token|token|key|api_key|signature)=)[^&\s]+', r'\1[REDACTED]', text)
    text = re.sub(r'(https?://)[^/\s@]+@', r'\1[REDACTED]@', text)
    text = re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]', '', text)
    text = ''.join(c for c in text if c in '\n\t' or ord(c) >= 32)
    return text if len(text) <= limit else text[:limit] + ' [truncated]'


def _safe_read(args) -> bool:
    # Deliberate allowlist: gh api fields/input imply POST, and unknown flags or
    # commands must not accidentally make a remote mutation replayable.
    if len(args) >= 2 and args[0] == 'api' and re.match(r'^repos/[^/]+/[^/]+/', args[1]):
        return args[2:] in ((), ('--method', 'GET'), ('-X', 'GET'))
    if len(args) >= 3 and args[:2] == ('release', 'view') and not args[2].startswith('-'):
        return not args[3:] or (len(args) == 5 and args[3] == '--json')
    # Re-downloading identical assets is safe only with explicit replacement of
    # a partial local file. No retry is added to upload/create/dispatch.
    return (len(args) == 8 and args[:2] == ('release', 'download')
            and not args[2].startswith('-') and args[3] == '--pattern'
            and args[5] == '--dir' and args[7] == '--clobber')


def _transient(stderr, *, timed_out=False) -> bool:
    # Classify the complete diagnostic: a permanent HTTP status beyond the
    # display limit must still veto retries. Only retained/emitted text is
    # redacted and shortened by safe_diagnostic().
    text = (stderr.decode('utf-8', errors='replace') if isinstance(stderr, bytes)
            else str(stderr or '')).lower()
    statuses = re.findall(r'\bhttp(?:/\d(?:\.\d)?)?\s+(\d{3})\b', text)
    if any(400 <= int(s) < 500 and s != '429' for s in statuses):
        return False
    return (timed_out or any(s in {'429', '500', '502', '503', '504'} for s in statuses)
            or any(s in text for s in ('tls handshake timeout', 'i/o timeout',
                                       'connection reset by peer', 'unexpected eof',
                                       'context deadline exceeded')))


class GhCommandError(subprocess.CalledProcessError):
    def __str__(self):
        return super().__str__() + f' Attempts: {self.attempts}. stderr: {self.stderr or "[empty]"}'


class GhTimeoutError(subprocess.TimeoutExpired):
    def __str__(self):
        return super().__str__() + f' Attempts: {self.attempts}. stderr: {self.stderr or "[empty]"}'


def error_details(error) -> dict:
    result = {'type': type(error).__name__, 'message': safe_diagnostic(str(error))}
    if isinstance(error, (subprocess.CalledProcessError, subprocess.TimeoutExpired)):
        command = error.cmd if isinstance(error.cmd, (list, tuple)) else [error.cmd]
        result.update(command=safe_diagnostic(' '.join(map(str, command))),
                      stderr=safe_diagnostic(error.stderr), attempts=getattr(error, 'attempts', 1),
                      retryable=getattr(error, 'retryable', False))
        if isinstance(error, subprocess.CalledProcessError):
            result['returncode'] = error.returncode
        else:
            result['timeout_seconds'] = error.timeout
    return result


def gh(*args: str, timeout: float | None = None, deadline: float | None = None) -> str:
    read = _safe_read(args)
    for attempt in range(1, 5):  # Initial read plus at most three same-command retries.
        attempt_timeout = timeout if timeout is not None else (30 if read else None)
        if deadline is not None:
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                raise TimeoutError('Release publication deadline exhausted; retain recovery files')
            attempt_timeout = min(attempt_timeout, remaining) if attempt_timeout is not None else remaining
        try:
            return subprocess.run(['gh', *args], check=True, text=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  timeout=attempt_timeout).stdout
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as original:
            retryable = read and _transient(original.stderr, timed_out=isinstance(original, subprocess.TimeoutExpired))
            command = [safe_diagnostic(x, 256) for x in ['gh', *args]]
            stderr = safe_diagnostic(original.stderr)
            if isinstance(original, subprocess.TimeoutExpired):
                error = GhTimeoutError(command, original.timeout, stderr=stderr)
            else:
                error = GhCommandError(original.returncode, command, stderr=stderr)
            error.attempts, error.retryable = attempt, retryable
            delay = 2 ** (attempt-1)
            if (not retryable or attempt == 4
                    or (deadline is not None and time.monotonic()+delay >= deadline)):
                raise error from None
            time.sleep(delay)


def ensure_release(tag: str, *, timeout: float | None = None,
                   deadline: float | None = None) -> None:
    try:
        gh('release', 'view', tag, timeout=timeout, deadline=deadline)
    except subprocess.CalledProcessError as error:
        # A failed read is not evidence that the release is absent. In particular,
        # a timeout/authorization/server error must never trigger a blind create.
        if str(error.stderr or '').strip().lower() != 'release not found':
            raise
        gh('release', 'create', tag, '--target', 'main', '--title', tag,
           '--notes', 'Append-only public market-data archive; see CAPTURE_V2.md.',
           '--latest=false', timeout=timeout, deadline=deadline)


def publish(tag: str, paths: list[Path], *, replace: bool = False,
            timeout: float | None = None, deadline: float | None = None) -> None:
    if not paths:
        return
    ensure_release(tag, timeout=timeout, deadline=deadline)
    for path in paths:
        args = ['release', 'upload', tag, str(path)]
        if replace:
            args.append('--clobber')
        gh(*args, timeout=timeout, deadline=deadline)


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


def upload_ready(root: Path, tag: str, *, stop: threading.Event | None = None,
                 budget_seconds: float | None = None, deadline: float | None = None) -> None:
    """Runs off the sampling thread. Files remain local after durable upload."""
    uploaded_path = root / '.uploaded.json'
    if budget_seconds is not None:
        budget_deadline = time.monotonic()+budget_seconds
        deadline = min(deadline, budget_deadline) if deadline is not None else budget_deadline
    uploaded = set(json.loads(uploaded_path.read_text())) if uploaded_path.exists() else set()
    for path in sorted(root.glob('*.jsonl.gz')):
        if stop is not None and stop.is_set():
            return
        checksum = path.with_suffix(path.suffix + '.sha256')
        if path.name in uploaded or not checksum.exists():
            continue
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError('Release upload budget exhausted; retain recovery files')
        # A retry may follow an ambiguous network result. Immutable local bytes
        # and a run-specific tag make replacing the same asset idempotent.
        publish(tag, [path, checksum], replace=True, timeout=60, deadline=deadline)
        uploaded.add(path.name)
        atomic_json(uploaded_path, sorted(uploaded))
    manifest = root / 'manifest.json'
    if manifest.exists():
        publish(tag, [manifest], replace=True, timeout=60, deadline=deadline)
