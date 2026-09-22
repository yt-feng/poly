"""Regression tests for RTDS control frames observed during the live smoke test."""
import unittest
from capture_v2 import decode_frame


class FrameTests(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(decode_frame('  \r\n'), ('empty', None))

    def test_ping_pong(self):
        self.assertEqual(decode_frame(' Pong\n'), ('pong', None))
        self.assertEqual(decode_frame('ping'), ('ping', None))

    def test_non_json(self):
        self.assertEqual(decode_frame('INVALID OPERATION')[0], 'non_json')

    def test_scalar_control(self):
        self.assertEqual(decode_frame('"pong"'), ('control', 'pong'))

    def test_json_object(self):
        self.assertEqual(decode_frame('{"payload":{"value":1}}'), ('data', {'payload': {'value': 1}}))

    def test_json_list(self):
        self.assertEqual(decode_frame('[{"a":1}]'), ('data', [{'a': 1}]))


if __name__ == '__main__':
    unittest.main()
