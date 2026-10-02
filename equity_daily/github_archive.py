"""Isolated, checksummed GitHub Release bundles. Never commit raw data to git."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import threading
import time
from .core import atomic_json, digest


def gh(*args):
    try:
        result = subprocess.run(['gh', *args], check=False, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired:
        raise RuntimeError('github_command_timeout') from None
    if result.returncode:
        # Preserve the status category without ever echoing credentials or subprocess output.
        error = result.stderr or ''
        code = next((str(n) for n in (404, 401, 403, 429, 500, 502, 503, 504) if re.search(r'\b'+str(n)+r'\b', error)), 'unknown')
        raise RuntimeError('github_http_' + code)
    return result.stdout


def release_info(tag):
    repo = os.environ.get('GH_REPO') or os.environ.get('GITHUB_REPOSITORY')
    if not repo or not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo):
        raise RuntimeError('GH_REPO_owner_name_required')
    return json.loads(gh('api', f'repos/{repo}/releases/tags/{tag}'))


def ensure_release(tag):
    if not re.fullmatch(r'equity-daily-v1-[A-Za-z0-9_.-]+', tag):
        raise ValueError('Refusing any non-equity release namespace')
    try:
        return release_info(tag)
    except RuntimeError as exc:
        if str(exc) != 'github_http_404':
            raise
    gh('release', 'create', tag, '--target', os.environ.get('GITHUB_SHA', 'main'),
       '--title', tag, '--latest=false', '--notes',
       'Independent equity_daily raw-data bundles. No BTC observations/strategies. '
       'Each tar contains native payload segments, manifests and SHA256 sidecars; '
       'see equity_daily/README.md for feed limitations and decoding.')
    return release_info(tag)


def bundle_ready(root: Path, max_bytes=128*1024*1024):
    """Tar only closed immutable files; few assets per checkpoint avoids release asset-count growth."""
    state_path = root / '.bundled.json'
    state = json.loads(state_path.read_text()) if state_path.exists() else {'files': [], 'bundles': []}
    known = set(state['files'])
    ready = sorted(p for p in root.iterdir() if p.is_file() and not p.is_symlink()
                   and p.name not in known
                   and ('.jsonl.gz' in p.name or p.name.startswith('manifest-'))
                   and not p.name.endswith(('.part', '.tmp')))
    groups, group, size = [], [], 0
    for path in ready:
        if group and size + path.stat().st_size > max_bytes:
            groups.append(group)
            group, size = [], 0
        group.append(path)
        size += path.stat().st_size
    if group:
        groups.append(group)
    for group in groups:
        seq = len(state['bundles']) + 1
        path = root / f'equity-daily-bundle-{seq:06d}.tar'
        if path.exists():
            raise RuntimeError('untracked_existing_bundle_do_not_overwrite')
        temp = Path(str(path) + '.part')
        with tarfile.open(temp, 'w') as tar:
            for member in group:
                tar.add(member, arcname=member.name, recursive=False)
        temp.replace(path)
        sha = digest(path)
        Path(str(path)+'.sha256').write_text(f'{sha}  {path.name}\n')
        state['files'].extend(p.name for p in group)
        state['bundles'].append(dict(file=path.name, sha256=sha, bytes=path.stat().st_size))
        atomic_json(state_path, state)
    return state['bundles']


_PUBLISH_LOCK = threading.Lock()


def publish_ready(root: Path, tag: str):
    # asyncio cancellation cannot interrupt a subprocess running in to_thread.
    # Serialize final publication with any still-running rolling checkpoint.
    with _PUBLISH_LOCK:
        return _publish_ready(root, tag)


def _publish_ready(root: Path, tag: str):
    bundles = bundle_ready(root)
    if not bundles:
        return
    info = ensure_release(tag)
    upload_path = root / '.uploaded.json'
    uploaded = set(json.loads(upload_path.read_text())) if upload_path.exists() else set()
    for item in bundles:
        name = item['file']
        if name in uploaded:
            continue
        path = root/name
        if digest(path) != item['sha256']:
            raise RuntimeError('local_bundle_checksum_mismatch')
        for attempt in range(3):
            try:
                # Same unique run/tag/file/bytes makes retry after an ambiguous response idempotent.
                gh('release', 'upload', tag, str(path), str(path)+'.sha256', '--clobber')
                remote = {x['name']: x for x in release_info(tag).get('assets', [])}
                asset = remote.get(name, {})
                if asset.get('size') != item['bytes'] or name+'.sha256' not in remote:
                    raise RuntimeError('github_asset_inventory_not_confirmed')
                if asset.get('digest') and asset['digest'] != 'sha256:'+item['sha256']:
                    raise RuntimeError('github_remote_digest_mismatch')
                uploaded.add(name)
                atomic_json(upload_path, sorted(uploaded))
                break
            except RuntimeError as exc:
                if attempt == 2 or str(exc) in ('github_http_401', 'github_http_403', 'github_remote_digest_mismatch'):
                    raise
                time.sleep(2**attempt)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--tag', required=True)
    args = parser.parse_args()
    publish_ready(args.root, args.tag)


if __name__ == '__main__':
    main()
