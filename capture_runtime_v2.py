"""Bounded process cleanup for the public-feed collector CLIs."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import os
import signal
import sys
import threading
import time

from archive_v2 import atomic_json


class CaptureCleanupError(RuntimeError):
    pass


def task_details(tasks):
    return {task.get_name(): [f'{frame.f_code.co_filename}:{frame.f_lineno}'
                             for frame in task.get_stack()] for task in tasks}


def cleanup(loop, executor, root, timeout):
    """Never use cancellation-waiting gather/wait_for during final cleanup.

    Tasks and async generators each get at most one third of the shared budget;
    executor shutdown receives the remainder. Its join runs outside the event
    loop because Python 3.11's shutdown_default_executor can block when cancelled.
    """
    started = time.monotonic()
    deadline = started + timeout
    failures = {}

    def loop_error(loop, context):
        failures.setdefault('event_loop_errors', []).append(
            {'message': context.get('message', ''), 'exception': repr(context.get('exception'))})
        loop.default_exception_handler(context)

    loop.set_exception_handler(loop_error)

    def wait_for_tasks(tasks):
        if not tasks:
            return set()
        _, pending = loop.run_until_complete(asyncio.wait(
            tasks, timeout=max(0, min(timeout / 3, deadline-time.monotonic()))))
        for task in tasks - pending:
            if not task.cancelled():
                error = task.exception()
                if error is not None:
                    failures.setdefault('task_errors', {})[task.get_name()] = repr(error)
        return pending

    pending = asyncio.all_tasks(loop)
    for task in pending:
        task.cancel()
    pending = wait_for_tasks(pending)
    if pending:
        failures['tasks'] = task_details(pending)

    generators = loop.create_task(loop.shutdown_asyncgens(), name='shutdown-async-generators')
    unfinished_generators = wait_for_tasks({generators})
    if unfinished_generators:
        failures['async_generators'] = task_details(unfinished_generators)
        generators.cancel()

    executor_error = []

    def join_executor():
        try:
            executor.shutdown(wait=True, cancel_futures=True)
        except BaseException as exc:
            executor_error.append(repr(exc))

    joiner = threading.Thread(target=join_executor, name='capture-executor-shutdown', daemon=True)
    joiner.start()
    joiner.join(timeout=max(0, deadline-time.monotonic()))
    executor_stopped = not joiner.is_alive()
    if not executor_stopped or executor_error:
        failures['executor'] = executor_error or ['Worker threads exceeded the cleanup deadline']

    remaining_tasks = asyncio.all_tasks(loop)
    if remaining_tasks:
        failures['remaining_tasks'] = task_details(remaining_tasks)
    # No unbounded gather or async-generator/default-executor shutdown follows.
    # Closing the loop abandons only already-diagnosed, uncooperative tasks.
    loop.close()
    asyncio.set_event_loop(None)
    diagnostics = {'cleanup_budget_seconds': timeout,
                   'elapsed_seconds': round(time.monotonic()-started, 3),
                   'failed': bool(failures), 'failures': failures,
                   'executor_stopped': executor_stopped}
    try:
        atomic_json(root/'process_cleanup.json', diagnostics)
    except OSError as exc:
        # A full/unwritable disk must not send a stuck executor into atexit's
        # unlimited join. Keep the explicit failure diagnostic on stderr too.
        failures['diagnostic_write'] = repr(exc)
        diagnostics['failed'] = True
    if failures:
        print('Capture process cleanup failed: '+json.dumps(diagnostics), file=sys.stderr, flush=True)
    if not executor_stopped or executor_error:
        # CPython otherwise joins non-daemon executor workers again at atexit.
        # Data/health/quality were finalized by Collector.run before this point.
        # This is an explicit, diagnosed FAILURE, never a successful shutdown.
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(1)
    if failures:
        raise CaptureCleanupError('Capture process cleanup failed; see process_cleanup.json')


def run_capture(collector, seconds, *, cleanup_timeout=30):
    """Run a collector and bound every process-cleanup phase on Python 3.11+."""
    if cleanup_timeout <= 0:
        raise ValueError('cleanup_timeout must be positive')
    loop = asyncio.new_event_loop()
    executor = ThreadPoolExecutor(thread_name_prefix='capture-io')
    loop.set_default_executor(executor)
    asyncio.set_event_loop(loop)
    main = loop.create_task(collector.run(seconds), name='capture-main')
    previous_sigint = None
    interrupts = 0

    def interrupt(signum, frame):
        nonlocal interrupts
        interrupts += 1
        if interrupts == 1 and not main.done():
            loop.call_soon_threadsafe(main.cancel)
        else:
            raise KeyboardInterrupt

    if (threading.current_thread() is threading.main_thread()
            and signal.getsignal(signal.SIGINT) is signal.default_int_handler):
        previous_sigint = signal.signal(signal.SIGINT, interrupt)
    try:
        return loop.run_until_complete(main)
    finally:
        if previous_sigint is not None:
            signal.signal(signal.SIGINT, previous_sigint)
        cleanup(loop, executor, collector.root, cleanup_timeout)
