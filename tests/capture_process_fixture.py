"""Offline subprocess fixtures for real CLI exit behavior; no network I/O."""
import asyncio
from functools import partial
from pathlib import Path
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import capture_v2
import capture_v3
from capture_runtime_v2 import run_capture

version, mode, output = sys.argv[1:]
module = capture_v2 if version == 'v2' else capture_v3
base = capture_v2.Collector if version == 'v2' else capture_v3.CollectorV3


class OfflineCollector(base):
    async def idle(self, *args, **kwargs):
        await asyncio.Future()

    poll_books = socket = depth_snapshots = idle

    async def discover(self):
        cancellations = 0
        while True:
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancellations += 1
                if mode != 'resist' and not (mode == 'recover' and cancellations == 1):
                    raise

    async def sample(self, duration):
        self.snapshot('btc', time.monotonic(), time.monotonic())
        if mode == 'interrupt':
            (self.root/'fixture_ready').write_text('ready')
            await asyncio.Future()
        if mode == 'asyncgen':
            async def stubborn_generator():
                try:
                    yield 1
                finally:
                    while True:
                        try:
                            await asyncio.Future()
                        except asyncio.CancelledError:
                            pass
            self.generator = stubborn_generator()
            await self.generator.__anext__()
        if mode in {'executor_stuck', 'executor_healthy'}:
            work = threading.Event().wait if mode == 'executor_stuck' else partial(time.sleep, .01)
            asyncio.create_task(asyncio.to_thread(work), name='synthetic-worker')
        await asyncio.sleep(.03)

    async def stop_tasks(self, tasks, grace=.01):
        return await super().stop_tasks(tasks, grace)


if version == 'v2':
    module.Collector = OfflineCollector
else:
    module.CollectorV3 = OfflineCollector
module.run_capture = partial(run_capture, cleanup_timeout=.3)
sys.argv = [f'capture_{version}.py', '--seconds', '1', '--output', output]
module.main()
