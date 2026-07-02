import os
import unittest

from src.remote_transport import (
    _ASKPASS_HELPERS,
    _cleanup_askpass_helpers,
    _openssh_askpass_env,
)


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
