import os
import unittest

from src.constants import (LOCAL_TAPE_BUDGET_BYTES, TAPE_STATUS_ACTIVE,
                           TAPE_STATUS_FULL, tape_budget_bytes, tape_is_full,
                           tape_status_reason_suffix)
from src.remote_transport import (
    _ASKPASS_HELPERS,
    _cleanup_askpass_helpers,
    _openssh_askpass_env,
)


class TapeBudgetTests(unittest.TestCase):
    def test_safety_budget_caps_large_capacities(self):
        # A 12288-GB LTO-8 capacity exceeds the safety budget, which wins.
        capacity, available = tape_budget_bytes(12288, used_bytes=0)
        self.assertEqual(capacity, LOCAL_TAPE_BUDGET_BYTES)
        self.assertEqual(available, LOCAL_TAPE_BUDGET_BYTES)

    def test_capacity_is_decimal_gb_and_reserved_is_subtracted(self):
        # 1000 GB is decimal (1e12 bytes), not GiB — the GiB reading
        # overstated capacity by ~7%.
        capacity, available = tape_budget_bytes(
            1000, used_bytes=200 * 1000**3, reserved_bytes=100 * 1000**3)
        self.assertEqual(capacity, 1000 * 1000**3)
        self.assertEqual(available, 700 * 1000**3)

    def test_available_never_goes_negative_and_no_capacity_uses_budget(self):
        _, available = tape_budget_bytes(100, used_bytes=200 * 1000**3)
        self.assertEqual(available, 0)
        capacity, _ = tape_budget_bytes(None, used_bytes=0)
        self.assertEqual(capacity, LOCAL_TAPE_BUDGET_BYTES)


class TapeStatusTests(unittest.TestCase):
    def test_tape_marked_full_has_no_available_bytes(self):
        # Tape_02 was retired read-only with ~5 TB of 12 TB used: the counters
        # say "room left", the status is what must win.
        capacity, available = tape_budget_bytes(
            12288, used_bytes=5 * 1000**4, status=TAPE_STATUS_FULL)
        self.assertEqual(capacity, LOCAL_TAPE_BUDGET_BYTES)
        self.assertEqual(available, 0)

    def test_active_and_missing_status_are_inert(self):
        # None covers a pre-011 database and unregistered tapes; the guard must
        # not change planning for either.
        expected = tape_budget_bytes(12288, used_bytes=1000**4)
        for status in (None, "", TAPE_STATUS_ACTIVE, "ACTIVE"):
            self.assertEqual(
                tape_budget_bytes(12288, used_bytes=1000**4, status=status),
                expected)

    def test_tape_is_full_normalises_case_and_whitespace(self):
        self.assertTrue(tape_is_full(" FULL "))
        self.assertFalse(tape_is_full(None))

    def test_status_reason_suffix(self):
        self.assertEqual(
            tape_status_reason_suffix({"status_reason": "read-only, PWE"}),
            " (read-only, PWE)")
        self.assertEqual(tape_status_reason_suffix({"status_reason": None}), "")
        self.assertEqual(tape_status_reason_suffix(None), "")


class AskpassHelperTests(unittest.TestCase):
    def setUp(self):
        _cleanup_askpass_helpers()

    def tearDown(self):
        _cleanup_askpass_helpers()

    def test_askpass_helper_is_reused_across_calls(self):
        # One helper script serves every SSH call regardless of password value,
        # so a long archive run must not leak one temp file per fetch.
        env1 = _openssh_askpass_env("secret")
        env2 = _openssh_askpass_env("a-different-secret")
        self.assertEqual(env1["SSH_ASKPASS"], env2["SSH_ASKPASS"])
        self.assertTrue(os.path.exists(env1["SSH_ASKPASS"]))
        self.assertEqual(_ASKPASS_HELPERS, {env1["SSH_ASKPASS"]})
        self.assertTrue(
            os.path.basename(env1["SSH_ASKPASS"]).startswith("lto_ssh_askpass_"))

    def test_askpass_password_travels_in_env_not_on_disk(self):
        secret = "s3cr3t-p@ss word"
        env = _openssh_askpass_env(secret)
        self.assertEqual(env["LTO_REMOTE_PASSWORD"], secret)
        with open(env["SSH_ASKPASS"], encoding="utf-8") as handle:
            body = handle.read()
        self.assertNotIn(secret, body)
        self.assertIn("LTO_REMOTE_PASSWORD", body)


if __name__ == "__main__":
    unittest.main()
