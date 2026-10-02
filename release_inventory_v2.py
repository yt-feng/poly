"""Bounded release discovery without REST's embedded release-asset inventories.

GraphQL Release.createdAt is the release object's creation time. It is not the
REST release created_at (the tag's commit time). The daily window uses the former
and includes the entire first page crossing its lower bound, including ties.
Assets are deliberately absent; callers must enumerate the REST asset endpoint.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
import re


PAGE_SIZE = 20
RELEASE_QUERY = (
    'query CaptureReleaseInventory($owner: String!, $name: String!, $cursor: String) '
    '{ repository(owner: $owner, name: $name) { nameWithOwner '
    'releases(first: 20, after: $cursor, orderBy: {field: CREATED_AT, direction: DESC}) '
    '{ nodes { databaseId tagName createdAt publishedAt isDraft } '
    'pageInfo { hasNextPage endCursor } } } }'
)
_NAME = r'[A-Za-z0-9_.-]+'
_CURSOR = r'[A-Za-z0-9+/=_-]{1,512}'


def release_inventory_args(repo: str, cursor: str | None = None) -> tuple[str, ...]:
    if not isinstance(repo, str) or not re.fullmatch(_NAME + '/' + _NAME, repo):
        raise ValueError('Invalid release inventory repository')
    if cursor is not None and (not isinstance(cursor, str) or not re.fullmatch(_CURSOR, cursor)):
        raise ValueError('Invalid release inventory cursor')
    owner, name = repo.split('/')
    # Raw string fields are intentional: numeric-looking repository names must
    # remain GraphQL String variables. Only the initial cursor is typed null.
    return ('api', 'graphql', '-f', 'query=' + RELEASE_QUERY,
            '-f', 'owner=' + owner, '-f', 'name=' + name,
            '-F' if cursor is None else '-f', 'cursor=' + ('null' if cursor is None else cursor))


def is_release_inventory_read(args) -> bool:
    """Recognize this exact static query and variable contract, never arbitrary GraphQL."""
    if (len(args) != 10 or not all(isinstance(arg, str) for arg in args)
            or args[:4] != ('api', 'graphql', '-f', 'query=' + RELEASE_QUERY)
            or args[4] != '-f' or args[6] != '-f'
            or not re.fullmatch('owner=' + _NAME, args[5])
            or not re.fullmatch('name=' + _NAME, args[7])):
        return False
    return (args[8:] == ('-F', 'cursor=null') or
            (args[8] == '-f' and re.fullmatch('cursor=' + _CURSOR, args[9]) is not None))


def _timestamp(value):
    if not isinstance(value, str) or not re.fullmatch(
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})', value):
        raise ValueError('Invalid release timestamp')
    return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)


def _object(value, keys, label):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise ValueError('Invalid release inventory ' + label)
    return value


def list_capture_releases(repo, cutoff, *, call=None, max_pages=100, on_page=None):
    """Return capture identities and an explicit bounded-discovery receipt.

    Reject partial GraphQL data, schema drift, identity conflicts, ordering
    violations and nonadvancing cursors rather than producing partial coverage.
    The full crossing page is retained; exact-cutoff ties on the next page must
    still be read. This window is not a full-history claim.
    """
    release_inventory_args(repo)  # Validate before any external call.
    if (not isinstance(cutoff, datetime) or cutoff.tzinfo is None
            or cutoff.utcoffset() is None or type(max_pages) is not int or not 1 <= max_pages <= 100):
        raise ValueError('Invalid bounded release discovery window')
    cutoff = cutoff.astimezone(timezone.utc)
    if call is None:
        from archive_v2 import gh
        call = gh
    cursor = None
    cursors = set()
    seen = {}
    tags = {}
    releases = []
    previous_time = None
    for page in range(1, max_pages + 1):
        if on_page is not None:
            on_page(page, cursor)
        response = json.loads(call(*release_inventory_args(repo, cursor), timeout=30))
        # Any GraphQL errors, including an errors+data partial result, are fatal.
        if isinstance(response, dict) and 'errors' in response:
            from archive_v2 import safe_diagnostic
            diagnostic = dict(errors=response['errors'], partial_data_present=response.get('data') is not None)
            raise ValueError('GraphQL release inventory errors: ' +
                             safe_diagnostic(json.dumps(diagnostic), 1000))
        response = _object(response, ('data',), 'response (partial data/errors rejected)')
        data = _object(response['data'], ('repository',), 'data')
        repository = _object(data['repository'], ('nameWithOwner', 'releases'), 'repository')
        if (not isinstance(repository['nameWithOwner'], str)
                or repository['nameWithOwner'].casefold() != repo.casefold()):
            raise ValueError('Release inventory repository identity mismatch')
        connection = _object(repository['releases'], ('nodes', 'pageInfo'), 'connection')
        batch = connection['nodes']
        info = _object(connection['pageInfo'], ('hasNextPage', 'endCursor'), 'pageInfo')
        if not isinstance(batch, list) or len(batch) > PAGE_SIZE or type(info['hasNextPage']) is not bool:
            raise ValueError('Invalid bounded release page')
        next_cursor = info['endCursor']
        if batch:
            if (not isinstance(next_cursor, str) or not re.fullmatch(_CURSOR, next_cursor)
                    or next_cursor == cursor or next_cursor in cursors):
                raise ValueError('Release pagination cursor did not advance')
            cursors.add(next_cursor)
        elif info['hasNextPage'] or next_cursor is not None or page > 1:
            raise ValueError('Release pagination returned an inconsistent empty page')

        added = 0
        for item in batch:
            _object(item, ('databaseId', 'tagName', 'createdAt', 'publishedAt', 'isDraft'), 'node')
            ident, tag = item['databaseId'], item['tagName']
            if (type(ident) is not int or ident <= 0 or not isinstance(tag, str)
                    or not tag or len(tag) > 1024 or any(ord(c) < 32 for c in tag)
                    or type(item['isDraft']) is not bool):
                raise ValueError('Invalid release identity')
            created = _timestamp(item['createdAt'])
            if item['publishedAt'] is not None:
                if _timestamp(item['publishedAt']) < created:
                    raise ValueError('Release publication predates creation')
            elif not item['isDraft']:
                raise ValueError('Published release has no publication timestamp')
            if previous_time is not None and created > previous_time:
                raise ValueError('Release creation order changed during pagination')
            previous_time = created
            if ident in seen:
                if seen[ident] != item:
                    raise ValueError('Release identity changed during pagination')
                continue
            if tag in tags and tags[tag] != ident:
                raise ValueError('Conflicting release tag identity')
            seen[ident] = item
            tags[tag] = ident
            added += 1
            if tag.startswith('capture-v2-'):
                if not re.fullmatch(r'capture-v2-[A-Za-z0-9_-]+', tag):
                    raise ValueError('Invalid capture release tag')
                releases.append(dict(id=ident, tag_name=tag,
                                     release_created_at=item['createdAt'],
                                     release_published_at=item['publishedAt'], is_draft=item['isDraft']))
        if batch and not added:
            raise RuntimeError('Release pagination did not add any identities')
        crossed_cutoff = previous_time is not None and previous_time < cutoff
        if not info['hasNextPage'] or crossed_cutoff:
            return releases, dict(
                api='graphql_release_metadata_v1', order='CREATED_AT_DESC', pages=page,
                page_size=PAGE_SIZE, unique_releases=len(seen), capture_releases=len(releases),
                cutoff_utc=cutoff.isoformat(), cutoff_field='GraphQL Release.createdAt',
                cutoff_semantics='Release object creation time, not REST tag commit created_at',
                entire_cutoff_page_included=True, release_connection_exhausted=not info['hasNextPage'],
                stop_reason='connection_exhausted' if not info['hasNextPage'] else 'created_before_cutoff',
                oldest_release_created_at=previous_time.isoformat() if previous_time is not None else None,
            )
        cursor = next_cursor
    raise RuntimeError('Release listing exceeded pagination bound; refusing incomplete inventory')
