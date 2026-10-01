"""Synthetic sparse release discovery, exact retry allowlist and failure artifacts."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from archive_v2 import gh, error_details
from daily_quality_v2 import main, list_release_assets
from release_inventory_v2 import (
    PAGE_SIZE, RELEASE_QUERY, is_release_inventory_read,
    release_inventory_args, list_capture_releases,
)


CUTOFF = datetime(2026, 1, 1, tzinfo=timezone.utc)


def node(ident, created='2026-01-01T00:00:00Z'):
    return dict(databaseId=ident, tagName=f'capture-v2-{ident}-1', createdAt=created,
                publishedAt=created, isDraft=False)


def response(nodes, more=False, cursor='last', repo='o/r'):
    return dict(data=dict(repository=dict(nameWithOwner=repo, releases=dict(
        nodes=nodes, pageInfo=dict(hasNextPage=more, endCursor=cursor)))))


def cli_ok(value):
    return subprocess.CompletedProcess([], 0, stdout=json.dumps(value), stderr='')


class ReleaseInventoryTests(unittest.TestCase):
    def call(self, pages):
        self.calls = []
        def fake(*args, **kwargs):
            self.calls.append((args, kwargs))
            return json.dumps(pages.pop(0))
        return fake

    def test_exact_static_query_uses_string_variables_and_no_asset_expansion(self):
        args = release_inventory_args('123/456')
        self.assertTrue(is_release_inventory_read(args))
        self.assertEqual(args[4:], ('-f', 'owner=123', '-f', 'name=456', '-F', 'cursor=null'))
        self.assertIn('releases(first: 20', RELEASE_QUERY)
        self.assertNotIn('assets', RELEASE_QUERY.lower())
        self.assertNotIn('mutation', RELEASE_QUERY)
        self.assertTrue(is_release_inventory_read(release_inventory_args('o/r', 'Y3Vyc29y==')))

    def test_created_at_not_commit_date_is_explicit_and_full_crossing_page_kept(self):
        pages = [response([node(1, '2026-01-02T00:00:00Z'), node(2)], True, 'a'),
                 response([node(3), node(4, '2025-12-31T23:59:59Z'),
                           node(5, '2025-12-31T20:00:00Z')], True, 'b')]
        found, receipt = list_capture_releases('o/r', CUTOFF, call=self.call(pages))
        self.assertEqual([r['id'] for r in found], [1, 2, 3, 4, 5])
        self.assertEqual(receipt['pages'], 2)
        self.assertEqual(receipt['cutoff_field'], 'GraphQL Release.createdAt')
        self.assertEqual(receipt['stop_reason'], 'created_before_cutoff')
        self.assertFalse(receipt['release_connection_exhausted'])
        self.assertTrue(receipt['entire_cutoff_page_included'])
        self.assertNotIn('created_at', found[0])
        self.assertEqual(self.calls[1][0], release_inventory_args('o/r', 'a'))

    def test_exact_cutoff_across_several_pages_does_not_omit_equal_time_releases(self):
        pages = [response([node(1)], True, 'a'), response([node(2)], True, 'b'),
                 response([node(3), node(4, '2025-12-31T23:59:59Z')], True, 'c')]
        found, receipt = list_capture_releases('o/r', CUTOFF, call=self.call(pages))
        self.assertEqual([r['id'] for r in found], [1, 2, 3, 4])
        self.assertEqual(receipt['pages'], 3)

    def test_timezone_equivalent_cutoff_ties_are_not_earlier(self):
        found, receipt = list_capture_releases('o/r', CUTOFF, call=self.call([
            response([node(1, '2026-01-01T01:00:00+01:00')], True, 'a'),
            response([node(2)], False, 'b')]))
        self.assertEqual(len(found), 2)
        self.assertEqual(receipt['stop_reason'], 'connection_exhausted')

    def test_other_tags_do_not_hide_following_capture_release(self):
        first = node(1, '2026-01-02T00:00:00Z')
        first['tagName'] = 'quality-v2-2026-01-01'
        found, receipt = list_capture_releases('o/r', CUTOFF, call=self.call([
            response([first], True, 'a'), response([node(2)], False, 'b')]))
        self.assertEqual([r['id'] for r in found], [2])
        self.assertEqual(receipt['unique_releases'], 2)

    def test_case_insensitive_repo_identity_and_draft_are_preserved(self):
        draft = node(1)
        draft.update(publishedAt=None, isDraft=True)
        found, _ = list_capture_releases('O/R', CUTOFF,
                                        call=self.call([response([draft], repo='o/r')]))
        self.assertTrue(found[0]['is_draft'])
        self.assertIsNone(found[0]['release_published_at'])

    def test_valid_initial_empty_connection_has_no_invented_release(self):
        found, receipt = list_capture_releases('o/r', CUTOFF,
                                              call=self.call([response([], cursor=None)]))
        self.assertEqual(found, [])
        self.assertEqual(receipt['unique_releases'], 0)
        self.assertTrue(receipt['release_connection_exhausted'])

    def test_partial_data_errors_and_wrong_repository_fail_before_asset_reads(self):
        partial = response([node(1)])
        partial['errors'] = [dict(message='synthetic partial resolver failure')]
        cases = [partial, dict(errors=[]), dict(data=None), dict(data=dict(repository=None)),
                 response([node(1)], repo='someone/else')]
        for value in cases:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    list_capture_releases('o/r', CUTOFF, call=self.call([value]))
                self.assertEqual(len(self.calls), 1)

    def test_graphql_error_diagnostic_is_retained_bounded_and_redacted(self):
        partial = response([node(1)])
        partial['errors'] = [dict(message='Resolver timed out ghp_syntheticsecret ' + 'x'*3000)]
        with self.assertRaisesRegex(ValueError, 'Resolver timed out') as caught:
            list_capture_releases('o/r', CUTOFF, call=self.call([partial]))
        self.assertNotIn('ghp_syntheticsecret', str(caught.exception))
        self.assertIn('[REDACTED]', str(caught.exception))
        self.assertLess(len(str(caught.exception)), 1100)
        self.assertEqual(len(self.calls), 1)

    def test_strict_node_and_page_schema_reject_truncation_wrong_types_and_assets(self):
        cases = []
        for key, value in [('databaseId', True), ('databaseId', 0), ('tagName', ''),
                           ('tagName', 'capture-v2-../escape'), ('tagName', 'a\nb'),
                           ('createdAt', '2026-01-01'), ('createdAt', '2026-13-01T00:00:00Z'),
                           ('publishedAt', None), ('isDraft', 0),
                           ('publishedAt', '2025-12-31T23:59:59Z')]:
            item = node(1); item[key] = value
            cases.append(response([item]))
        missing = node(1); del missing['createdAt']
        cases.extend([response([missing]), response([None]), response({}),
                      response([node(i) for i in range(1, PAGE_SIZE + 2)]),
                      response([dict(node(1), assets=[dict(id=9)])]),
                      response([node(1)], more=1), response([node(1)], cursor=None)])
        for value in cases:
            with self.subTest(value=str(value)[:150]), self.assertRaises(ValueError):
                list_capture_releases('o/r', CUTOFF, call=self.call([value]))

    def test_out_of_order_within_or_across_pages_fails(self):
        newer = node(2, '2026-01-02T00:00:00Z')
        for pages in [[response([node(1), newer])],
                      [response([node(1)], True, 'a'), response([newer], False, 'b')]]:
            with self.subTest(pages=pages), self.assertRaisesRegex(ValueError, 'order'):
                list_capture_releases('o/r', CUTOFF, call=self.call(pages))

    def test_conflicting_duplicate_id_and_tag_fail(self):
        changed = node(1); changed['tagName'] = 'capture-v2-changed'
        duplicate_tag = node(2); duplicate_tag['tagName'] = node(1)['tagName']
        for item in [changed, duplicate_tag]:
            with self.subTest(item=item), self.assertRaises(ValueError):
                list_capture_releases('o/r', CUTOFF, call=self.call([
                    response([node(1)], True, 'a'), response([item], False, 'b')]))

    def test_identical_boundary_duplicate_is_deduplicated_when_page_adds_identity(self):
        found, receipt = list_capture_releases('o/r', CUTOFF, call=self.call([
            response([node(1)], True, 'a'), response([node(1), node(2)], False, 'b')]))
        self.assertEqual([r['id'] for r in found], [1, 2])
        self.assertEqual(receipt['unique_releases'], 2)

    def test_duplicate_only_page_does_not_count_as_progress_even_with_new_cursor(self):
        with self.assertRaisesRegex(RuntimeError, 'identities'):
            list_capture_releases('o/r', CUTOFF, call=self.call([
                response([node(1)], True, 'a'), response([node(1)], False, 'b')]))

    def test_cursor_repeat_cycle_invalid_and_contradictory_empty_fail(self):
        tails = [[response([node(2)], False, 'a')],
                 [response([node(2)], True, 'b'), response([node(3)], False, 'a')],
                 [response([node(2)], False, '')], [response([], False, None)],
                 [response([], True, None)]]
        for tail in tails:
            with self.subTest(tail=tail), self.assertRaises(ValueError):
                list_capture_releases('o/r', CUTOFF,
                                      call=self.call([response([node(1)], True, 'a'), *tail]))

    def test_page_cap_raises_instead_of_returning_partial_inventory(self):
        with self.assertRaisesRegex(RuntimeError, 'pagination bound'):
            list_capture_releases('o/r', CUTOFF, max_pages=2, call=self.call([
                response([node(1)], True, 'a'), response([node(2)], True, 'b')]))

    def test_invalid_arguments_fail_before_call(self):
        with patch('archive_v2.gh') as call:
            for repo, cutoff, cap in [('bad', CUTOFF, 1), ('o/r', datetime(2026, 1, 1), 1),
                                      ('o/r', CUTOFF, True), ('o/r', CUTOFF, 101)]:
                with self.subTest(repo=repo, cutoff=cutoff, cap=cap), self.assertRaises(ValueError):
                    list_capture_releases(repo, cutoff, max_pages=cap)
            call.assert_not_called()

    def test_thousands_of_assets_do_not_expand_metadata_but_still_all_paginate(self):
        # A release may have thousands of assets. Its metadata remains one small
        # node; complete asset enumeration happens only on the separate endpoint.
        found, _ = list_capture_releases('o/r', CUTOFF, call=self.call([response([node(1)])]))
        self.assertLess(len(json.dumps(response([node(1)]))), 500)
        self.assertTrue(all('assets' not in arg for arg in self.calls[0][0]))
        inventory = [dict(id=i, name=f'file-{i}', size=i) for i in range(1, 1002)]
        calls = []
        def assets(*args, **kwargs):
            calls.append(args)
            page = int(args[1].rsplit('page=', 1)[1])
            return json.dumps(inventory[(page-1)*100:page*100])
        listing, pages = list_release_assets('o/r', found[0]['id'], call=assets)
        self.assertEqual((len(listing), pages), (1001, 11))
        self.assertEqual(listing, inventory)


class GraphQLRetryTests(unittest.TestCase):
    def failure(self, stderr='HTTP 504: gateway timeout'):
        return subprocess.CalledProcessError(1, ['gh', 'api', 'graphql'], stderr=stderr)

    def test_static_inventory_504_retries_only_exact_command_four_times(self):
        args = release_inventory_args('o/r')
        with patch('archive_v2.subprocess.run', side_effect=self.failure()) as run, patch('archive_v2.time.sleep') as sleep:
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                gh(*args)
        self.assertEqual([c.args[0] for c in run.call_args_list], [['gh', *args]]*4)
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [1, 2, 4])
        self.assertEqual(error_details(caught.exception)['attempts'], 4)

    def test_transient_then_success_keeps_cursor_and_complete_response(self):
        args = release_inventory_args('o/r', 'samecursor==')
        expected = response([node(1)])
        with patch('archive_v2.subprocess.run', side_effect=[self.failure(), cli_ok(expected)]) as run, patch('archive_v2.time.sleep'):
            self.assertEqual(json.loads(gh(*args)), expected)
        self.assertEqual([c.args[0] for c in run.call_args_list], [['gh', *args]]*2)

    def test_other_graphql_queries_mutations_and_variable_injection_never_replay(self):
        original = release_inventory_args('o/r')
        commands = [('api', 'graphql'), original + ('--paginate',), original + ('-f', 'unknown=x'),
                    original[:8] + ('-F', 'cursor=123'), original[:8] + ('-f', 'cursor=bad cursor'),
                    original[:4] + ('-f', 'owner=o/else', *original[6:])]
        for query in ['mutation { deleteRepository(input:{repositoryId:"x"}) { clientMutationId } }',
                      RELEASE_QUERY + ' query Unknown { viewer { login } }',
                      RELEASE_QUERY.replace('isDraft', 'isDraft releaseAssets(first:100){totalCount}'),
                      RELEASE_QUERY.replace('first: 20', 'first: 100')]:
            commands.append((*original[:3], 'query='+query, *original[4:]))
        for args in commands:
            with self.subTest(args=args), patch('archive_v2.subprocess.run', side_effect=self.failure()) as run, patch('archive_v2.time.sleep') as sleep:
                self.assertFalse(is_release_inventory_read(args))
                with self.assertRaises(subprocess.CalledProcessError):
                    gh(*args)
                self.assertEqual(run.call_count, 1)
                sleep.assert_not_called()

    def test_permanent_and_unknown_errors_remain_single_attempt(self):
        for stderr in ['HTTP 401', 'HTTP 403', 'HTTP 404', 'HTTP 422', 'GraphQL: unknown field', 'unknown']:
            with self.subTest(stderr=stderr), patch('archive_v2.subprocess.run', side_effect=self.failure(stderr)) as run, patch('archive_v2.time.sleep') as sleep:
                with self.assertRaises(subprocess.CalledProcessError):
                    gh(*release_inventory_args('o/r'))
                self.assertEqual(run.call_count, 1)
                sleep.assert_not_called()

    def test_second_page_504_retains_actual_stage_without_fake_quality(self):
        with tempfile.TemporaryDirectory() as d:
            output = Path(d)/'output'
            args = SimpleNamespace(date='2026-01-02', assets='btc', output=output,
                                   threshold=.95, publish=False, check=False, quiet=True)
            first = cli_ok(response([node(1)], True, 'a'))
            with patch.dict(os.environ, {'GH_REPO': 'o/r'}), patch('archive_v2.subprocess.run', side_effect=[first]+[self.failure()]*4) as run, patch('archive_v2.time.sleep'), patch('daily_quality_v2.report') as report:
                with self.assertRaises(subprocess.CalledProcessError):
                    main(args)
            failure = json.loads((output/'failure.json').read_text())
            self.assertEqual(failure['stage'], 'release_inventory')
            self.assertEqual(failure['context']['page'], '2')
            self.assertEqual(failure['context']['cursor'], 'a')
            self.assertEqual(failure['error']['attempts'], 4)
            self.assertIn('HTTP 504', failure['error']['stderr'])
            self.assertFalse(failure['quality_report_complete'])
            self.assertFalse((output/'quality.json').exists())
            self.assertNotIn('daily', failure)
            report.assert_not_called()
            self.assertEqual(run.call_count, 5)

    def test_partial_graphql_result_never_publishes_or_measures_partial_coverage(self):
        with tempfile.TemporaryDirectory() as d:
            output = Path(d)/'output'
            args = SimpleNamespace(date='2026-01-02', assets='btc', output=output,
                                   threshold=.95, publish=True, check=True, quiet=True)
            partial = response([node(1)])
            partial['errors'] = [dict(message='synthetic resolver timeout')]
            with patch.dict(os.environ, {'GH_REPO': 'o/r'}), patch('archive_v2.subprocess.run', return_value=cli_ok(partial)) as run, patch('daily_quality_v2.report') as report, patch('daily_quality_v2.publish') as publish:
                with self.assertRaises(ValueError):
                    main(args)
            self.assertEqual(run.call_count, 1)
            report.assert_not_called()
            publish.assert_not_called()
            self.assertFalse((output/'quality.json').exists())
            self.assertEqual(json.loads((output/'failure.json').read_text())['stage'], 'release_inventory')


if __name__ == '__main__':
    unittest.main()
