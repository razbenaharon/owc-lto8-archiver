import json
import os
import tempfile
import unittest

from src.maintenance_lock import (
    clear_stale_maintenance_lock,
    ensure_no_active_maintenance_lock,
)
from src.remote_transport import _ASKPASS_HELPERS, _openssh_askpass_env


class AskpassHelperTests(unittest.TestCase):
    def test_askpass_helper_uses_randomized_temp_names(self):
        env1 = _openssh_askpass_env("secret")
        env2 = _openssh_askpass_env("secret")
        try:
            self.assertNotEqual(env1["SSH_ASKPASS"], env2["SSH_ASKPASS"])
            self.assertTrue(os.path.exists(env1["SSH_ASKPASS"]))
            self.assertTrue(os.path.exists(env2["SSH_ASKPASS"]))
            self.assertIn(env1["SSH_ASKPASS"], _ASKPASS_HELPERS)
            self.assertIn(env2["SSH_ASKPASS"], _ASKPASS_HELPERS)
        finally:
            for path in (env1["SSH_ASKPASS"], env2["SSH_ASKPASS"]):
                try:
                    os.remove(path)
                except OSError:
                    pass
                _ASKPASS_HELPERS.discard(path)


class MaintenanceLockTests(unittest.TestCase):
    def test_dead_pid_lock_is_cleared(self):
        with tempfile.TemporaryDirectory() as tmp:
            lock_path = os.path.join(tmp, "archive.db.maintenance.lock")
            with open(lock_path, "w", encoding="utf-8") as handle:
                json.dump({"pid": 99999999}, handle)
            self.assertTrue(clear_stale_maintenance_lock(lock_path))
            self.assertFalse(os.path.exists(lock_path))

    def test_live_pid_lock_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            lock_path = os.path.join(tmp, "archive.db.maintenance.lock")
            with open(lock_path, "w", encoding="utf-8") as handle:
                json.dump({"pid": os.getpid()}, handle)
            with self.assertRaises(RuntimeError):
                ensure_no_active_maintenance_lock(lock_path)
            self.assertTrue(os.path.exists(lock_path))


if __name__ == "__main__":
    unittest.main()
