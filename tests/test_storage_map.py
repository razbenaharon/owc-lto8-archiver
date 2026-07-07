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


# A small legacy du --max-depth=2 raw log; parsing trims non-shared-data
# folders to the top layer.
SAMPLE_RAWLOG = "\n".join([
    "# storage-map raw log",
    "# server: so01.iem.technion.ac.il",
    "# generated_at: 2026-07-01T02:00:00",
    "# depth: 2",
    "##### MOUNT: /strg/E #####",
    "##### DF: 2199023255552 1099511627776 1099511627776 50% #####",
    "500G\t/strg/E/alice/project_x",
    "100G\t/strg/E/alice/scratch",
    "700G\t/strg/E/alice",
    "300G\t/strg/E/bob",
    "1.0T\t/strg/E",
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
            (700 * 1024**3, "/strg/E/alice"),
            (500 * 1024**3, "/strg/E/alice/project_x"),
            (300 * 1024**3, "/strg/E/bob"),
            (1024**4, "/strg/E"),
        ]
        root = _build_tree(entries, "/strg/E")
        self.assertEqual(root.path, "/strg/E")
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
            path = os.path.join(tmp, "so01_latest.rawlog")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(SAMPLE_RAWLOG)
            result = parse_raw_log(path)

        self.assertEqual(result.host, "so01.iem.technion.ac.il")
        self.assertEqual(result.server, "so01")
        self.assertEqual(result.depth, 2)
        self.assertEqual(len(result.mounts), 2)

        by_mount = {m.mount: m for m in result.mounts}
        self.assertEqual(by_mount["/strg/E"].total, 1024**4)
        self.assertEqual(by_mount["/strg/E"].capacity_bytes, 2 * 1024**4)
        self.assertEqual(by_mount["/strg/E"].free_bytes, 1024**4)
        free_percent = by_mount["/strg/E"].free_percent
        assert free_percent is not None
        self.assertAlmostEqual(free_percent, 50.0)
        self.assertEqual(by_mount["/data"].total, 2 * 1024**3)
        alice = next(c for c in by_mount["/strg/E"].root.children
                     if c.name == "alice")
        self.assertEqual(alice.children, [])
        # Grand total aggregates across mounts.
        self.assertEqual(result.total, 1024**4 + 2 * 1024**3)

    def test_dashboard_mount_percentage_is_free_space_left(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "so01_latest.rawlog")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(SAMPLE_RAWLOG)
            result = parse_raw_log(path)

        mount = next(m for m in result.mounts if m.mount == "/strg/E")
        text = _mount_value_text(mount, result.total)
        self.assertIn("1.0 TiB used", text)
        self.assertIn("1.0 TiB left", text)
        self.assertIn("50.0% left", text)
        self.assertAlmostEqual(_mount_bar_width(mount, result.total), 50.0)


class RemoteLauncherScriptTests(unittest.TestCase):
    def test_mounts_are_injected_from_config_and_low_priority(self):
        srv = ServerConfig("so01", "so01.example", "user", "",
                           ["/strg/E", "/data"])
        script = _remote_launcher_script(srv, 2)
        # Mounts come straight from the ServerConfig (not hardcoded constants).
        self.assertIn("/strg/E", script)
        self.assertIn("/data", script)
        # Low-priority, metadata-only, depth-limited, sentinel-terminated.
        self.assertIn("ionice", script)
        self.assertIn("nice", script)
        # Byte-exact du output (not -h) so tape-coverage math is precise.
        self.assertIn("du -x -B1", script)
        self.assertIn("df -B1 -P", script)
        self.assertIn("##### DF:", script)
        self.assertIn("--max-depth=1", script)
        self.assertIn("/strg/E/shared-data", script)
        self.assertNotIn("--max-depth=2", script)
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
            ServerConfig("so01", "so01.example", "user", "", ["/data"]),
            ServerConfig("so02", "so02.example", "user", "", ["/data"]),
        ]
        notifier = _FakeNotifier()

        def fake_ssh(user, host, cmd, password="", timeout=None):
            if host == "so01.example":
                return SimpleNamespace(returncode=0, stdout="LAUNCHED\n", stderr="")
            return SimpleNamespace(returncode=1, stdout="", stderr="nope")

        with tempfile.TemporaryDirectory() as tmp:
            smcfg = self._config(tmp, servers)
            with mock.patch("storage_map.lib.core._ssh_run", side_effect=fake_ssh):
                rc = scan(smcfg, servers, notifier=notifier)

        self.assertEqual(rc, 1)
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("Launched: so01", notifier.messages[0])
        self.assertIn("Failed: so02", notifier.messages[0])
        self.assertNotIn("/data", notifier.messages[0])

    def test_status_notifies_meaningful_state_change_once(self):
        server = ServerConfig("so01", "so01.example", "user", "", ["/data"])
        notifier = _FakeNotifier()

        with tempfile.TemporaryDirectory() as tmp:
            smcfg = self._config(tmp, [server])
            scan_started = "2026-07-01T02:00:00"
            with mock.patch("storage_map.lib.core._ssh_run",
                            return_value=SimpleNamespace(
                                returncode=0, stdout="LAUNCHED\n", stderr="")):
                scan(smcfg, [server], notifier=None)
            # Pin the manifest timestamp so the assertion is deterministic.
            path = os.path.join(tmp, "so01.pending.json")
            with open(path, "r", encoding="utf-8") as handle:
                manifest = json.load(handle)
            manifest["started_at"] = scan_started
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(manifest, handle)

            with mock.patch("storage_map.lib.core._remote_status", return_value="DONE"):
                self.assertEqual(status(smcfg, [server], notifier=notifier), 0)
                self.assertEqual(status(smcfg, [server], notifier=notifier), 0)

        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("so01 is DONE", notifier.messages[0])
        self.assertIn(scan_started, notifier.messages[0])

    def test_fetch_sends_aggregate_outcome_summary(self):
        servers = [
            ServerConfig("so01", "so01.example", "user", "", ["/data"]),
            ServerConfig("so02", "so02.example", "user", "", ["/data"]),
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
        self.assertIn("Fetched: so01", notifier.messages[0])
        self.assertIn("Skipped: so02(PENDING)", notifier.messages[0])
        self.assertNotIn("/data", notifier.messages[0])


if __name__ == "__main__":
    unittest.main()
