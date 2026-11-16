"""Regression tests for audit M10: the stall/overrun watchdog and scan timeout.

The watchdog must still abort a wedged or runaway fetch (incident 008 and the
2026-07-15 hang), but it must not cancel a healthy one. Three false positives
are pinned here, each driven through the real monitor: with a scripted clock
and scripted staging sizes where the timing is long, and through the real
``_fetch_chunk`` with a one-second stall timeout where the interaction with
the governor or with a resumed fetch dir is the point.
"""
import os
import shutil
import subprocess
import threading
import time
import unittest
from types import SimpleNamespace
from unittest import mock

from src import remote_staging as rs
from src import remote_transport, scanning
from src.remote_staging import (RemoteChunkStager, _FetchPauseState,
                                _FetchProgressClock, _fetch_watchdog_action)
from test_audit_fetch_pack_integrity import (FakeDB, FakeRemote, make_host,
                                             make_rows, _real_tar_available)

PLENTY = SimpleNamespace(total=10**13, used=0, free=10**13)


class _Clock:
    def __init__(self):
        self.now = 0.0

    def time(self):
        return self.now


class _ScriptedStop:
    """A stop event whose ``wait`` plays the next scripted instant."""

    def __init__(self, steps, clock, apply):
        self.steps = list(steps)
        self.clock = clock
        self.apply = apply

    def wait(self, _interval):
        if not self.steps:
            return True
        when, state = self.steps.pop(0)
        self.clock.now = float(when)
        self.apply(state)
        return False


class _InlineThread:
    """Captures the monitor so the test can run it on its own thread."""

    captured = []

    def __init__(self, target=None, **_kwargs):
        self.target = target

    def start(self):
        type(self).captured.append(self.target)


def run_monitor(steps, *, total_bytes, stall_timeout=600, abort_factor=2.0,
                tar_parts=None, baseline_bytes=0):
    """Drive the real fetch monitor through ``steps`` on a fake clock.

    ``steps`` is ``[(time, state), ...]``; ``state`` is the staged size (plain
    fetch) or a ``{part_path: size}`` dict (Stored TAR). Returns the abort
    event and the instant at which it fired (or ``None``).
    """
    clock = _Clock()
    current = {"state": 0 if tar_parts is None else {}}
    host = SimpleNamespace(
        staging_dir="synthetic", notifier=None,
        fetch_abort_factor=abort_factor, fetch_stall_timeout=stall_timeout)
    stager = RemoteChunkStager(host)
    abort = threading.Event()
    fired_at = []

    real_set = abort.set

    def record_set():
        fired_at.append(clock.now)
        real_set()

    abort.set = record_set
    stop = _ScriptedStop(
        steps, clock, lambda state: current.__setitem__("state", state))

    def snapshot(_parts, _loose_dirs):
        parts = dict(current["state"])
        return {"parts": parts, "loose_bytes": 0,
                "total_bytes": sum(parts.values())}

    _InlineThread.captured = []
    fake_threading = SimpleNamespace(
        Thread=_InlineThread, Lock=threading.Lock, Event=threading.Event)
    with mock.patch.object(rs, "time", SimpleNamespace(time=clock.time)), \
            mock.patch.object(rs, "threading", fake_threading), \
            mock.patch.object(rs, "_dir_tree_size",
                              side_effect=lambda _p: current["state"]), \
            mock.patch.object(rs, "_staging_progress_snapshot",
                              side_effect=snapshot), \
            mock.patch.object(rs.shutil, "disk_usage", return_value=PLENTY), \
            mock.patch.object(rs, "_progress_line"), \
            mock.patch.object(rs, "send_best_effort"):
        kwargs = {}
        if tar_parts is not None:
            kwargs["tar_part_paths"] = list(tar_parts)
        if baseline_bytes:
            kwargs["baseline_bytes"] = baseline_bytes
        stager._start_fetch_monitor(
            stop, abort, "synthetic-fetch", total_bytes, **kwargs)
        _InlineThread.captured[0]()
    return abort, (fired_at[0] if fired_at else None)


class LaterContainerIsNotAStallTests(unittest.TestCase):
    """M10a: a Stored TAR chunk's later containers start again from zero."""

    def _two_container_script(self, second_until):
        steps = [(60 * n, {"c0.part": 100 * n}) for n in range(1, 6)]
        steps.append((360, {}))                      # container 0 published
        tick = 420
        size = 0
        while tick <= second_until:
            size += 10
            steps.append((tick, {"c1.part": size}))
            tick += 60
        return steps, tick, size

    def test_second_container_growing_below_the_first_is_healthy(self):
        steps, _tick, size = self._two_container_script(second_until=2400)
        self.assertLess(size, 500, "the second part must stay below the first")
        abort, fired_at = run_monitor(
            steps, total_bytes=10_000, tar_parts=["c0.part", "c1.part"])
        self.assertFalse(abort.is_set(),
                         f"healthy second container aborted at t={fired_at}")

    def test_second_container_that_really_stops_is_still_aborted(self):
        steps, tick, size = self._two_container_script(second_until=900)
        steps += [(tick + 60 * n, {"c1.part": size}) for n in range(0, 14)]
        abort, fired_at = run_monitor(
            steps, total_bytes=10_000, tar_parts=["c0.part", "c1.part"])
        self.assertTrue(abort.is_set())
        self.assertGreaterEqual(fired_at - (tick - 60), 600)


class ResumeBaselineTests(unittest.TestCase):
    """M10c: bytes an interrupted run left behind are not this run's overrun."""

    def test_overrun_is_measured_from_the_resume_baseline(self):
        # 2000 B already staged, 1000 B still planned, delivered on plan.
        steps = [(10, 2000), (20, 2400), (30, 2800), (40, 3000)]
        abort, _ = run_monitor(steps, total_bytes=1000, baseline_bytes=2000)
        self.assertFalse(abort.is_set())

    def test_a_real_overrun_beyond_the_baseline_still_aborts(self):
        # Incident 008's safety net stays: >2x the remaining plan is runaway.
        steps = [(10, 2000), (20, 3000), (30, 4001)]
        abort, fired_at = run_monitor(
            steps, total_bytes=1000, baseline_bytes=2000)
        self.assertTrue(abort.is_set())
        self.assertEqual(fired_at, 30)

    def test_clock_reports_growth_relative_to_baseline(self):
        clock = _FetchProgressClock(0, baseline_bytes=700)
        self.assertEqual(clock.observe(700, 5), 0)
        self.assertEqual(clock.observe(950, 6), 250)
        self.assertEqual(clock.observe(100, 7), 0)     # never negative


class PauseIsNotAStallTests(unittest.TestCase):
    """M10b at the decision level, on a fake clock."""

    def test_stall_clock_restarts_when_a_pause_ends(self):
        fake = _Clock()
        pause = _FetchPauseState(clock=fake.time)
        progress = _FetchProgressClock(0)
        progress.observe(500, 10)

        def action(now):
            paused, resumed_at = pause.snapshot()
            cur = progress.observe(500, now, paused=paused,
                                   resumed_at=resumed_at)
            return _fetch_watchdog_action(
                cur=cur, last_growth_at=progress.last_growth_at, now=now,
                total_bytes=10_000, abort_factor=2.0, stall_timeout=600,
                free_bytes=10**13, reserve_bytes=0)

        with pause.paused():
            for now in (300, 900, 1500, 2100):          # 35 min of back-pressure
                self.assertIsNone(action(now), now)
            fake.now = 2200
        self.assertIsNone(action(2300))                 # 100 s after resuming
        self.assertIsNone(action(2700))
        self.assertEqual(action(2801), "stall")         # 601 s with no data

    def test_unpaused_silence_is_still_a_stall(self):
        progress = _FetchProgressClock(0)
        progress.observe(500, 10)
        progress.observe(500, 700)
        self.assertEqual(
            _fetch_watchdog_action(
                cur=500, last_growth_at=progress.last_growth_at, now=700,
                total_bytes=10_000, abort_factor=2.0, stall_timeout=600,
                free_bytes=10**13, reserve_bytes=0),
            "stall")


class _PausingGovernor:
    """Holds the producer once, between the first and second tar stream."""

    def __init__(self, remote, seconds):
        self.remote = remote
        self.seconds = seconds
        self.paused_once = False

    def wait_or_pause(self, stage, phase, **_kwargs):
        if ((stage, phase) == ("fetch", "continue")
                and len(self.remote.requests) == 1 and not self.paused_once):
            self.paused_once = True
            time.sleep(self.seconds)
        return True


@unittest.skipUnless(_real_tar_available(), "needs the Windows tar extractor")
class FetchChunkWatchdogTests(unittest.TestCase):
    """The same false positives through the real fetch and monitor thread."""

    def setUp(self):
        import tempfile
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.staging = self._tmp.name
        self.fetch_dir = os.path.join(self.staging, "_fetch_s0001_000")

    def _build(self, files):
        self.remote = FakeRemote(files)
        self.db = FakeDB(make_rows(files))
        self.host = make_host(self.staging, self.db)

    def _fetch(self, fetch_impl=None):
        with mock.patch("src.remote_staging._remote_tar_fetch",
                        side_effect=fetch_impl or self.remote.fetch), \
                mock.patch("src.remote_staging._remote_source_status_probe",
                           side_effect=self.remote.probe):
            return self.host._fetch_chunk(
                1, 0, self.db.get_chunk_files(1, 0), self.fetch_dir)

    def test_governor_pause_longer_than_the_stall_timeout_is_not_a_stall(self):
        self._build({"a.bin": b"A" * 100, "b.bin": b"B" * 100})
        self.host.metadata_batch_size = 1              # two tar streams
        self.host.fetch_stall_timeout = 1
        self.host.governor = _PausingGovernor(self.remote, seconds=5)
        ok, missing, count = self._fetch()
        self.assertTrue(self.host.governor.paused_once)
        self.assertTrue(ok, "a governor pause was treated as a wedged stream")
        self.assertEqual((missing, count), ([], 2))

    def test_resume_with_most_of_the_chunk_already_staged_is_not_an_overrun(self):
        files = {f"f{n}.bin": bytes([65 + n]) * 1000 for n in range(4)}
        self._build(files)
        # An interrupted run already fetched three of the four files.
        os.makedirs(self.fetch_dir)
        for name in ("f0.bin", "f1.bin", "f2.bin"):
            with open(os.path.join(self.fetch_dir, name), "wb") as handle:
                handle.write(files[name])

        def slow_fetch(user, host, base, rel_paths, dest, **kwargs):
            # A stream that takes a few seconds, and dies if the watchdog
            # aborts it - exactly what the real ssh/tar pair does.
            if kwargs["abort_evt"].wait(3.5):
                return False, "fetch aborted by the staging watchdog"
            return self.remote.fetch(user, host, base, rel_paths, dest)

        ok, missing, count = self._fetch(slow_fetch)
        self.assertEqual(self.remote.requests, [["f3.bin"]])
        self.assertTrue(ok, "a resumed fetch was aborted as an overrun")
        self.assertEqual((missing, count), ([], 4))


class ScanCannotHangForeverTests(unittest.TestCase):
    """M10: the remote scan has a time limit and SSH keepalives."""

    def test_frontier_scanner_never_runs_an_unbounded_listing(self):
        for given in (None, 0):
            with self.subTest(timeout=given):
                scanner = scanning.DirectoryFrontierScanner(
                    "u", "synthetic.invalid", timeout=given)
                done = subprocess.CompletedProcess([], 0, "OBS 1:1:1\0", "")
                with mock.patch.object(scanning, "_ssh_run",
                                       return_value=done) as run:
                    scanner.list_directory("/vault/a")
                    scanner.observe("/vault/a")
                for call in run.call_args_list:
                    self.assertEqual(
                        call.kwargs["timeout"],
                        scanning.DEFAULT_SCAN_LISTING_TIMEOUT_SECONDS)
        self.assertGreaterEqual(
            scanning.DEFAULT_SCAN_LISTING_TIMEOUT_SECONDS, 600)

    def test_factory_built_like_production_is_bounded_and_configurable(self):
        from src.scan_frontier import build_frontier_scanner_factory
        default = build_frontier_scanner_factory(
            remote_user="u", remote_host="h", remote_password="",
            skipped_tracker=None, ui=None)(None)
        self.assertEqual(default.timeout,
                         scanning.DEFAULT_SCAN_LISTING_TIMEOUT_SECONDS)
        tuned = build_frontier_scanner_factory(
            remote_user="u", remote_host="h", remote_password="",
            skipped_tracker=None, ui=None, timeout=7200)(None)
        self.assertEqual(tuned.timeout, 7200)

    def test_hung_listing_fails_cleanly_and_discards_partial_output(self):
        scanner = scanning.DirectoryFrontierScanner("u", "synthetic.invalid")

        def hung(cmd, **kwargs):
            self.assertIsNotNone(kwargs.get("timeout"))
            raise subprocess.TimeoutExpired(cmd, kwargs["timeout"],
                                            output="f 5 /vault/a/half")

        with mock.patch.object(remote_transport.subprocess, "run",
                               side_effect=hung):
            with self.assertRaisesRegex(RuntimeError, "timed out"):
                scanner.list_directory("/vault/a")

    def test_every_ssh_command_form_carries_keepalives(self):
        seen = []

        def fake_run(cmd, **_kwargs):
            seen.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, "", "")

        with mock.patch.object(remote_transport.subprocess, "run",
                               side_effect=fake_run):
            remote_transport._ssh_run("u", "h", "true")
            with mock.patch.object(remote_transport, "_has_command",
                                   side_effect=lambda name: name == "ssh"):
                remote_transport._ssh_run("u", "h", "true", password="pw")
            with mock.patch.object(remote_transport, "_has_command",
                                   return_value=True):
                remote_transport._ssh_run("u", "h", "true", password="pw")
        self.assertEqual(len(seen), 3)
        for cmd in seen:
            self.assertIn("ServerAliveInterval=15", cmd)
            self.assertIn("ServerAliveCountMax=4", cmd)
            self.assertIn("ConnectTimeout=30", cmd)


if __name__ == "__main__":
    unittest.main()
