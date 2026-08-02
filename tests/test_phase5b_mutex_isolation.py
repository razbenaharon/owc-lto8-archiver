"""Phase 5B: the test-only ownership override is explicit and pytest-gated.

Proves that no environment variable can silently repoint the PRODUCTION mutex,
that per-process pytest isolation still works, and that genuine cross-process
contention can still be exercised with an explicit shared name.
"""
import os
import subprocess
import sys
import textwrap
import time
import unittest
from unittest import mock

from src import ltfs_ownership as own
from src.ltfs_ownership import LtfsOwnership

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROD_NAME = "OWC_LTO8_LTFS_OWNERSHIP_drive_0000000000"

HOLDER = textwrap.dedent(
    """
    import sys, time
    sys.path.insert(0, r"{root}")
    from src.ltfs_ownership import LtfsOwnership
    o = LtfsOwnership(name="{name}", timeout=5)
    o.acquire("holder")
    print("ACQUIRED", flush=True)
    time.sleep({hold})
    o.release(operation="holder")
    """
)


def _spawn_holder(name, hold=4.0):
    return subprocess.Popen(
        [sys.executable, "-c", HOLDER.format(root=PROJECT_ROOT, name=name,
                                             hold=hold)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def _wait_acquired(proc, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            return False
        if "ACQUIRED" in line:
            return True
    return False


class MutexOverrideHardeningTests(unittest.TestCase):
    def test_production_default_name_ignores_the_env_var(self):
        # Even with the test env var set, a fresh production-style ownership
        # object (no explicit name) resolves the config identity, not the env.
        with mock.patch.dict(os.environ, {"LTO_TEST_OWNERSHIP_ID": "sneaky"}):
            o = LtfsOwnership()
            self.assertEqual(o.name, own.default_mutex_name())
            self.assertNotIn("sneaky", o.name)

    def test_default_mutex_name_is_stable_and_env_free(self):
        with mock.patch.dict(os.environ, {"LTO_TEST_OWNERSHIP_ID": "whatever"}):
            self.assertEqual(own.default_mutex_name(), own.default_mutex_name())
            self.assertNotIn("whatever", own.default_mutex_name())

    def test_activation_refuses_outside_pytest(self):
        # Guard: simulate a production process (pytest not importable, no pytest
        # env markers) and prove the override refuses to run.
        with mock.patch.object(own, "_running_under_pytest",
                               return_value=False):
            with self.assertRaises(RuntimeError):
                own.activate_test_ownership_isolation("anything")

    def test_pytest_isolation_is_active_for_this_process(self):
        # conftest called activate_test_ownership_isolation() at import, so the
        # singleton carries this process's unique, deterministic test identity.
        self.assertTrue(
            own.OWNERSHIP.name.startswith("OWC_LTO8_LTFS_OWNERSHIP_test_"))
        self.assertNotEqual(own.OWNERSHIP.name, PROD_NAME)

    def test_unrelated_workers_get_distinct_names(self):
        a = own.activate_test_ownership_isolation("worker_a")
        b = own.activate_test_ownership_isolation("worker_b")
        self.assertNotEqual(a, b)
        # restore this process's default isolation id so later tests are stable
        own.activate_test_ownership_isolation()

    def test_unrelated_names_do_not_contend(self):
        # Two ownership objects with different explicit names never block.
        o1 = LtfsOwnership(name="OWC_LTO8_P5B_unrelated_1", timeout=5)
        o2 = LtfsOwnership(name="OWC_LTO8_P5B_unrelated_2", timeout=5)
        o1.acquire("a")
        try:
            o2.acquire("b")            # must not block on o1
            o2.release(operation="b")
        finally:
            o1.release(operation="a")

    def test_shared_explicit_name_still_contends_across_processes(self):
        name = f"OWC_LTO8_P5B_shared_{os.getpid()}"
        holder = _spawn_holder(name, hold=4.0)
        try:
            self.assertTrue(_wait_acquired(holder),
                            "subprocess never acquired the shared mutex")
            mine = LtfsOwnership(name=name, timeout=1)
            t0 = time.time()
            with self.assertRaises(own.LtfsOwnershipError):
                mine.acquire("contend", timeout=1)   # held by the subprocess
            self.assertLess(time.time() - t0, 15)
        finally:
            holder.wait(timeout=20)


if __name__ == "__main__":
    unittest.main()
