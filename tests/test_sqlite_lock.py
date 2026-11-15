"""Cross-process archiver lock for the SQLite backend, with real
subprocesses.  Pure offline: only temporary directories are touched."""
import errno
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from src import sqlite_lock as sl

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

HOLDER = r"""
import os
import sys
from src.sqlite_lock import ArchiverLock, ArchiverLockError
lock = ArchiverLock(sys.argv[1])
try:
    lock.acquire()
except ArchiverLockError as exc:
    print("REFUSED " + type(exc).__name__ + " " + str(exc), flush=True)
    sys.exit(3)
print("READY " + lock.token + " " + str(os.getpid()), flush=True)
command = sys.stdin.readline().strip()
if command == "verify":
    lock.verify()
    print("VERIFIED", flush=True)
    sys.stdin.readline()
lock.release()
print("RELEASED", flush=True)
"""

PROBE = r"""
import sys
from src.sqlite_lock import probe_held
print("HELD" if probe_held(sys.argv[1]) else "FREE", flush=True)
"""


class LockTestCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="lto_lock_")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.db = os.path.join(self.dir, "catalog.db")
        self.lock_file = self.db + sl.LOCK_SUFFIX

    def lock(self, **kw):
        lock = sl.ArchiverLock(self.db, **kw)
        self.addCleanup(lock.release)
        return lock

    def spawn(self, script, *args):
        proc = subprocess.Popen(
            [sys.executable, "-c", script, *args], cwd=PROJECT_ROOT,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True)
        self.addCleanup(self._reap, proc)
        return proc

    @staticmethod
    def _reap(proc):
        if proc.poll() is None:
            proc.kill()
        try:
            proc.communicate(timeout=30)
        except Exception:
            pass

    def spawn_holder(self):
        proc = self.spawn(HOLDER, self.db)
        line = proc.stdout.readline().strip()
        self.assertTrue(line.startswith("READY "),
                        f"holder did not start: {line!r} "
                        f"{proc.stderr.read() if proc.poll() is not None else ''}")
        # The real interpreter pid: a venv python.exe on Windows is a
        # launcher whose child is the process that actually holds the lock.
        _ready, token, pid = line.split()
        return proc, token, int(pid)

    @staticmethod
    def kill_hard(pid):
        if os.name == "nt":
            subprocess.run(["taskkill", "/F", "/PID", str(pid)],
                           capture_output=True, check=False)
        else:
            os.kill(pid, signal.SIGKILL)

    def run_probe(self):
        proc = self.spawn(PROBE, self.db)
        out, err = proc.communicate(timeout=60)
        self.assertEqual(proc.returncode, 0, err)
        return out.strip()


class SingleProcessTests(LockTestCase):
    def test_acquire_verify_release(self):
        lock = self.lock()
        self.assertFalse(lock.held)
        token = lock.acquire()
        self.assertTrue(lock.held)
        self.assertEqual(token, lock.token)
        self.assertTrue(lock.verify())
        self.assertTrue(lock.verify(token))
        owner = sl.read_owner(self.db)
        self.assertEqual(owner["token"], token)
        self.assertEqual(owner["pid"], os.getpid())
        self.assertEqual(owner["db_path"], os.path.abspath(self.db))
        self.assertIn("acquired_at", owner)
        self.assertIn("host", owner)
        lock.release()
        self.assertFalse(lock.held)
        self.assertEqual(sl.read_owner(self.db), {})
        lock.release()                      # idempotent
        with self.assertRaises(sl.ArchiverLockLost):
            lock.verify()

    def test_owner_json_is_readable_while_locked(self):
        lock = self.lock()
        token = lock.acquire()
        with open(self.lock_file, "rb") as handle:
            record = json.loads(handle.read(sl.OWNER_RECORD_BYTES))
        self.assertEqual(record["token"], token)
        self.assertEqual(os.path.getsize(self.lock_file),
                         sl.OWNER_RECORD_BYTES)

    def test_context_manager_and_reacquire(self):
        with sl.ArchiverLock(self.db) as lock:
            self.assertTrue(lock.verify())
            first = lock.token
        self.assertFalse(sl.probe_held(self.db))
        with sl.ArchiverLock(self.db) as lock:
            self.assertNotEqual(lock.token, first)

    def test_second_lock_in_same_process_is_refused(self):
        first = self.lock()
        first.acquire()
        second = self.lock()
        with self.assertRaises(sl.ArchiverLockHeld):
            second.acquire()
        with self.assertRaises(sl.ArchiverLockError):
            first.acquire()
        self.assertTrue(first.verify())
        first.release()
        second.acquire()
        self.assertTrue(second.verify())

    def test_same_process_os_lock_conflicts_without_the_registry(self):
        """The OS lock itself (not just the registry) excludes a second
        handle, even inside one process."""
        first = self.lock()
        first.acquire()
        with mock.patch.dict(sl._HELD, clear=True):
            self.assertTrue(sl.probe_held(self.db))
            with self.assertRaises(sl.ArchiverLockHeld) as ctx:
                sl.ArchiverLock(self.db, retry_delay=0.01).acquire()
            self.assertEqual(ctx.exception.holder.get("pid"), os.getpid())
        self.assertTrue(first.verify())

    def test_token_mismatch_fails_verify(self):
        lock = self.lock()
        lock.acquire()
        with self.assertRaises(sl.ArchiverLockLost):
            lock.verify("someone-else")
        self.assertTrue(lock.verify())
        # Another party rewrites the owner record at the start of the file.
        forged = json.dumps({"token": "forged", "pid": 1}).encode()
        fd = os.open(self.lock_file, os.O_RDWR | getattr(os, "O_BINARY", 0))
        try:
            os.write(fd, forged.ljust(sl.OWNER_RECORD_BYTES))
        finally:
            os.close(fd)
        with self.assertRaises(sl.ArchiverLockLost) as ctx:
            lock.verify()
        self.assertIn("token", str(ctx.exception))
        lock.release()
        # release() must not blank a record that is not its own.
        self.assertEqual(sl.read_owner(self.db).get("token"), "forged")

    def test_registry_disagreement_fails_verify(self):
        lock = self.lock()
        lock.acquire()
        with mock.patch.dict(sl._HELD, clear=True):
            with self.assertRaises(sl.ArchiverLockLost):
                lock.verify()
        self.assertTrue(lock.verify())

    def test_invalid_handle_fails_verify(self):
        lock = self.lock()
        lock.acquire()
        os.close(lock._fd)                  # simulate a lost handle
        with self.assertRaises(sl.ArchiverLockLost):
            lock.verify()
        lock._fd = None
        lock.release()
        with sl._HELD_GUARD:
            sl._HELD.clear()

    def test_replaced_lock_file_is_detected(self):
        lock = self.lock()
        lock.acquire()
        if os.name == "nt":
            # Windows will not let the held file be deleted or replaced.
            with self.assertRaises(OSError):
                os.remove(self.lock_file)
            self.assertTrue(lock.verify())
            return
        record = sl.read_owner(self.db)
        os.remove(self.lock_file)
        with self.assertRaises(sl.ArchiverLockLost):
            lock.verify()
        with open(self.lock_file, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(record))
        with self.assertRaises(sl.ArchiverLockLost):
            lock.verify()

    def test_probe_is_non_destructive(self):
        self.assertFalse(sl.probe_held(self.db))
        self.assertFalse(os.path.exists(self.lock_file))    # never created
        lock = self.lock()
        token = lock.acquire()
        self.assertTrue(sl.probe_held(self.db))
        self.assertEqual(sl.read_owner(self.db)["token"], token)
        self.assertTrue(lock.verify())
        lock.release()
        self.assertFalse(sl.probe_held(self.db))
        self.assertTrue(os.path.exists(self.lock_file))

    def test_brief_contention_is_retried(self):
        real = sl._try_lock
        calls = []

        def flaky(fd):
            calls.append(fd)
            if len(calls) < 3:
                raise OSError(errno.EACCES, "busy")
            return real(fd)

        lock = self.lock(retry_delay=0.2)
        with mock.patch.object(sl, "_try_lock", flaky), \
                mock.patch.object(sl.time, "sleep") as sleep:
            lock.acquire()
        self.assertEqual(len(calls), 3)
        self.assertEqual([c.args for c in sleep.call_args_list],
                         [(0.2,), (0.2,)])
        self.assertTrue(lock.verify())

    def test_gives_up_after_three_attempts(self):
        lock = self.lock()
        with mock.patch.object(
                sl, "_try_lock", side_effect=OSError(errno.EACCES, "busy")
        ) as attempt, mock.patch.object(sl.time, "sleep"):
            with self.assertRaises(sl.ArchiverLockHeld) as ctx:
                lock.acquire()
        self.assertEqual(attempt.call_count, 3)
        self.assertIn("holder unknown", str(ctx.exception))
        self.assertFalse(lock.held)


class FailClosedTests(LockTestCase):
    def test_missing_directory(self):
        lock = sl.ArchiverLock(os.path.join(self.dir, "nope", "catalog.db"))
        with self.assertRaises(sl.ArchiverLockError) as ctx:
            lock.acquire()
        self.assertNotIsInstance(ctx.exception, sl.ArchiverLockHeld)
        self.assertIn("Cannot open", str(ctx.exception))
        self.assertFalse(lock.held)

    def test_lock_path_that_cannot_be_opened(self):
        os.mkdir(self.lock_file)            # a directory where the file goes
        lock = sl.ArchiverLock(self.db)
        with self.assertRaises(sl.ArchiverLockError):
            lock.acquire()
        self.assertFalse(lock.held)
        self.assertTrue(sl.probe_held(self.db))     # doubt => "held"

    @unittest.skipIf(os.name == "nt" or (
        hasattr(os, "geteuid") and os.geteuid() == 0),
        "needs POSIX permissions and a non-root user")
    def test_unwritable_directory(self):
        locked_dir = os.path.join(self.dir, "ro")
        os.mkdir(locked_dir)
        os.chmod(locked_dir, 0o500)
        self.addCleanup(os.chmod, locked_dir, 0o700)
        lock = sl.ArchiverLock(os.path.join(locked_dir, "catalog.db"))
        with self.assertRaises(sl.ArchiverLockError):
            lock.acquire()
        self.assertFalse(lock.held)

    @unittest.skipIf(os.name == "nt", "flock semantics are POSIX-only")
    def test_verify_never_reacquires_a_dropped_os_lock(self):
        """Review finding: verify() used to re-flock its own descriptor, which
        silently took a dropped lock again instead of reporting the loss."""
        import fcntl
        lock = sl.ArchiverLock(self.db)
        lock.acquire()
        self.addCleanup(lock.release)
        self.assertTrue(lock.verify())
        fcntl.flock(lock._fd, fcntl.LOCK_UN)        # the OS lock is gone
        with self.assertRaises(sl.ArchiverLockLost):
            lock.verify()
        self.assertFalse(sl.probe_held(self.db))    # and was not re-taken

    def test_unwritable_location_reported_by_the_os(self):
        lock = sl.ArchiverLock(self.db)
        with mock.patch.object(
                sl.os, "open",
                side_effect=PermissionError(errno.EACCES, "denied")):
            with self.assertRaises(sl.ArchiverLockError) as ctx:
                lock.acquire()
            self.assertTrue(sl.probe_held(self.db))
        self.assertIn("Refusing to run", str(ctx.exception))
        self.assertFalse(lock.held)

    def test_non_local_path(self):
        lock = sl.ArchiverLock(self.db)
        with mock.patch.object(sl, "is_network_path", return_value=True):
            with self.assertRaises(sl.ArchiverLockError) as ctx:
                lock.acquire()
            self.assertTrue(sl.probe_held(self.db))
        self.assertIn("non-local", str(ctx.exception))
        self.assertFalse(os.path.exists(self.lock_file))

    def test_lock_failure_that_is_not_contention(self):
        lock = sl.ArchiverLock(self.db)
        with mock.patch.object(
                sl, "_try_lock", side_effect=OSError(errno.ENOLCK, "no locks")):
            with self.assertRaises(sl.ArchiverLockError) as ctx:
                lock.acquire()
            self.assertTrue(sl.probe_held(self.db))
        self.assertNotIsInstance(ctx.exception, sl.ArchiverLockHeld)

    def test_ownership_record_failure_releases_everything(self):
        lock = sl.ArchiverLock(self.db)
        with mock.patch.object(
                sl, "_write_owner_fd", side_effect=OSError(errno.EIO, "io")):
            with self.assertRaises(sl.ArchiverLockError):
                lock.acquire()
        self.assertFalse(lock.held)
        self.assertFalse(sl.probe_held(self.db))


class CrossProcessTests(LockTestCase):
    def test_second_process_is_refused_and_names_the_holder(self):
        lock = self.lock()
        lock.acquire()
        proc = self.spawn(HOLDER, self.db)
        out, err = proc.communicate(timeout=60)
        self.assertEqual(proc.returncode, 3, err)
        self.assertIn("REFUSED ArchiverLockHeld", out)
        self.assertIn(f"pid {os.getpid()}", out)
        self.assertTrue(lock.verify())      # the refusal changed nothing

    def test_parent_is_refused_while_a_child_holds(self):
        proc, token, pid = self.spawn_holder()
        self.assertTrue(sl.probe_held(self.db))
        self.assertEqual(sl.read_owner(self.db)["token"], token)
        with self.assertRaises(sl.ArchiverLockHeld) as ctx:
            self.lock(retry_delay=0.05).acquire()
        self.assertEqual(ctx.exception.holder["pid"], pid)
        self.assertIn(f"pid {pid}", str(ctx.exception))
        # A clean release hands the lock over.
        proc.stdin.write("release\n")
        proc.stdin.flush()
        self.assertEqual(proc.stdout.readline().strip(), "RELEASED")
        proc.wait(timeout=60)
        self.assertFalse(sl.probe_held(self.db))
        lock = self.lock()
        lock.acquire()
        self.assertTrue(lock.verify())

    def test_lock_vanishes_when_the_holder_is_killed(self):
        proc, token, pid = self.spawn_holder()
        self.assertTrue(sl.probe_held(self.db))
        self.kill_hard(pid)                 # no chance to clean up
        proc.wait(timeout=60)
        deadline = time.monotonic() + 30
        while sl.probe_held(self.db) and time.monotonic() < deadline:
            time.sleep(0.1)                 # the OS frees it at process exit
        self.assertFalse(sl.probe_held(self.db))
        # The stale owner record is still there, and harmless.
        self.assertEqual(sl.read_owner(self.db)["token"], token)
        lock = self.lock()
        new_token = lock.acquire()
        self.assertNotEqual(new_token, token)
        self.assertEqual(sl.read_owner(self.db)["pid"], os.getpid())
        self.assertTrue(lock.verify())

    def test_probe_from_another_process(self):
        self.assertEqual(self.run_probe(), "FREE")
        self.assertFalse(os.path.exists(self.lock_file))
        proc, _token, _pid = self.spawn_holder()
        self.assertEqual(self.run_probe(), "HELD")
        # The probe must not have disturbed the holder.
        proc.stdin.write("verify\n")
        proc.stdin.flush()
        self.assertEqual(proc.stdout.readline().strip(), "VERIFIED")
        self.assertEqual(self.run_probe(), "HELD")
        proc.stdin.write("release\n")
        proc.stdin.flush()
        self.assertEqual(proc.stdout.readline().strip(), "RELEASED")
        proc.wait(timeout=60)
        self.assertEqual(self.run_probe(), "FREE")


if __name__ == "__main__":
    unittest.main()
