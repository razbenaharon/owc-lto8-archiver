import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from storage_map.lib.core import (
    _build_tree,
    _remote_launcher_script,
    fetch,
    parse_raw_log,
    parse_size,
    scan,
    ServerConfig,
    status,
    StorageMapConfig,
)
from storage_map.lib.dashboard import _mount_bar_width, _mount_value_text


# A small du -h --max-depth=2 raw log covering two mounts, using tabs like du.
SAMPLE_RAWLOG = "\n".join([
    "# storage-map raw log",
    "# server: srv01.example.edu",
    "# generated_at: 2026-07-01T02:00:00",
    "# depth: 2",
    "##### MOUNT: /vault/E #####",
    "##### DF: 2199023255552 1099511627776 1099511627776 50% #####",
    "500G\t/vault/E/alice/project_x",
    "100G\t/vault/E/alice/scratch",
    "700G\t/vault/E/alice",
    "300G\t/vault/E/bob",
    "1.0T\t/vault/E",
    "##### MOUNT: /data #####",
    "##### DF: 4294967296 2147483648 2147483648 50% #####",
    "2.0G\t/data/logs",
    "2.0G\t/data",
    "##### END #####",
    "",
])


class ParseSizeTests(unittest.TestCase):
    def test_units_normalize_to_bytes_base_1024(self):
        self.assertEqual(parse_size("512"), 512)
        self.assertEqual(parse_size("1K"), 1024)
        self.assertEqual(parse_size("4.0K"), 4096)
        self.assertEqual(parse_size("1.5G"), int(1.5 * 1024**3))
        self.assertEqual(parse_size("1.0T"), 1024**4)

    def test_tolerates_comma_decimal_and_iec_suffix(self):
        self.assertEqual(parse_size("4,0K"), 4096)
        self.assertEqual(parse_size("2GiB"), 2 * 1024**3)

    def test_blank_or_garbage_is_zero(self):
        self.assertEqual(parse_size(""), 0)
        self.assertEqual(parse_size("total"), 0)
        self.assertEqual(parse_size(None), 0)


class BuildTreeTests(unittest.TestCase):
    def test_hierarchy_and_aggregation(self):
        entries = [
            (700 * 1024**3, "/vault/E/alice"),
            (500 * 1024**3, "/vault/E/alice/project_x"),
            (300 * 1024**3, "/vault/E/bob"),
            (1024**4, "/vault/E"),
        ]
        root = _build_tree(entries, "/vault/E")
        self.assertEqual(root.path, "/vault/E")
        self.assertEqual(root.size, 1024**4)
        # Depth-1 children: alice + bob attach to the mount root.
        names = sorted(c.name for c in root.children)
        self.assertEqual(names, ["alice", "bob"])
        alice = next(c for c in root.children if c.name == "alice")
        # Depth-2 child nests under its parent, not the root.
        self.assertEqual([c.name for c in alice.children], ["project_x"])


class ParseRawLogTests(unittest.TestCase):
    def test_parses_sections_header_and_totals(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "srv01_latest.rawlog")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(SAMPLE_RAWLOG)
            result = parse_raw_log(path)

        self.assertEqual(result.host, "srv01.example.edu")
        self.assertEqual(result.server, "srv01")
        self.assertEqual(result.depth, 2)
        self.assertEqual(len(result.mounts), 2)

        by_mount = {m.mount: m for m in result.mounts}
        self.assertEqual(by_mount["/vault/E"].total, 1024**4)
        self.assertEqual(by_mount["/vault/E"].capacity_bytes, 2 * 1024**4)
        self.assertEqual(by_mount["/vault/E"].free_bytes, 1024**4)
        self.assertAlmostEqual(by_mount["/vault/E"].free_percent, 50.0)
        self.assertEqual(by_mount["/data"].total, 2 * 1024**3)
        # Grand total aggregates across mounts.
        self.assertEqual(result.total, 1024**4 + 2 * 1024**3)

    def test_dashboard_mount_percentage_is_free_space_left(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "srv01_latest.rawlog")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(SAMPLE_RAWLOG)
            result = parse_raw_log(path)

        mount = next(m for m in result.mounts if m.mount == "/vault/E")
        self.assertIn("50.0% left", _mount_value_text(mount, result.total))
        self.assertAlmostEqual(_mount_bar_width(mount, result.total), 50.0)


class RemoteLauncherScriptTests(unittest.TestCase):
    def test_mounts_are_injected_from_config_and_low_priority(self):
        srv = ServerConfig("srv01", "srv01.example", "user", "",
                           ["/vault/E", "/data"])
        script = _remote_launcher_script(srv, 2)
        # Mounts come straight from the ServerConfig (not hardcoded constants).
        self.assertIn("/vault/E", script)
        self.assertIn("/data", script)
        # Low-priority, metadata-only, depth-limited, sentinel-terminated.
        self.assertIn("ionice", script)
        self.assertIn("nice", script)
        self.assertIn("df -B1 -P", script)
        self.assertIn("##### DF:", script)
        self.assertIn("--max-depth=2", script)
        self.assertIn("scan.sentinel", script)


class _FakeNotifier:
    def __init__(self):
        self.messages = []

    def send(self, text):
        self.messages.append(text)
        return True


class StorageMapNotificationTests(unittest.TestCase):
    def _config(self, tmp, servers):
        return StorageMapConfig(
            output_dir=tmp,
            dashboard_dir=os.path.join(tmp, "dashboard"),
            depth=2,
            poll_timeout=10,
            servers=servers,
        )

    def test_scan_sends_aggregate_launch_summary(self):
        servers = [
            ServerConfig("srv01", "srv01.example", "user", "", ["/data"]),
            ServerConfig("srv02", "srv02.example", "user", "", ["/data"]),
        ]
        notifier = _FakeNotifier()

        def fake_ssh(user, host, cmd, password="", timeout=None):
            if host == "srv01.example":
                return SimpleNamespace(returncode=0, stdout="LAUNCHED\n", stderr="")
            return SimpleNamespace(returncode=1, stdout="", stderr="nope")

        with tempfile.TemporaryDirectory() as tmp:
            smcfg = self._config(tmp, servers)
            with mock.patch("storage_map.lib.core._ssh_run", side_effect=fake_ssh):
                rc = scan(smcfg, servers, notifier=notifier)

        self.assertEqual(rc, 1)
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("Launched: srv01", notifier.messages[0])
        self.assertIn("Failed: srv02", notifier.messages[0])
        self.assertNotIn("/data", notifier.messages[0])

    def test_status_notifies_meaningful_state_change_once(self):
        server = ServerConfig("srv01", "srv01.example", "user", "", ["/data"])
        notifier = _FakeNotifier()

        with tempfile.TemporaryDirectory() as tmp:
            smcfg = self._config(tmp, [server])
            scan_started = "2026-07-01T02:00:00"
            with mock.patch("storage_map.lib.core._ssh_run",
                            return_value=SimpleNamespace(
                                returncode=0, stdout="LAUNCHED\n", stderr="")):
                scan(smcfg, [server], notifier=None)
            # Pin the manifest timestamp so the assertion is deterministic.
            path = os.path.join(tmp, "srv01.pending.json")
            with open(path, "r", encoding="utf-8") as handle:
                manifest = json.load(handle)
            manifest["started_at"] = scan_started
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(manifest, handle)

            with mock.patch("storage_map.lib.core._remote_status", return_value="DONE"):
                self.assertEqual(status(smcfg, [server], notifier=notifier), 0)
                self.assertEqual(status(smcfg, [server], notifier=notifier), 0)

        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("srv01 is DONE", notifier.messages[0])
        self.assertIn(scan_started, notifier.messages[0])

    def test_fetch_sends_aggregate_outcome_summary(self):
        servers = [
            ServerConfig("srv01", "srv01.example", "user", "", ["/data"]),
            ServerConfig("srv02", "srv02.example", "user", "", ["/data"]),
        ]
        notifier = _FakeNotifier()

        def fake_scp(user, host, remote_out, local_path, password=""):
            with open(local_path, "w", encoding="utf-8") as handle:
                handle.write(SAMPLE_RAWLOG)
            return 0

        with tempfile.TemporaryDirectory() as tmp:
            smcfg = self._config(tmp, servers)
            with mock.patch("storage_map.lib.core._remote_status",
                            side_effect=["DONE", "PENDING"]):
                with mock.patch("storage_map.lib.core._scp_fetch_file",
                                side_effect=fake_scp):
                    rc = fetch(smcfg, servers, notifier=notifier)

        self.assertEqual(rc, 0)
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("Fetched: srv01", notifier.messages[0])
        self.assertIn("Skipped: srv02(PENDING)", notifier.messages[0])
        self.assertNotIn("/data", notifier.messages[0])


if __name__ == "__main__":
    unittest.main()
