import os
import tempfile
import unittest

from src.storage_map import (
    _build_tree,
    _remote_launcher_script,
    parse_raw_log,
    parse_size,
    ServerConfig,
)


# A small du -h --max-depth=2 raw log covering two mounts, using tabs like du.
SAMPLE_RAWLOG = "\n".join([
    "# storage-map raw log",
    "# server: so01.iem.technion.ac.il",
    "# generated_at: 2026-07-01T02:00:00",
    "# depth: 2",
    "##### MOUNT: /strg/E #####",
    "500G\t/strg/E/alice/project_x",
    "100G\t/strg/E/alice/scratch",
    "700G\t/strg/E/alice",
    "300G\t/strg/E/bob",
    "1.0T\t/strg/E",
    "##### MOUNT: /data #####",
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
        self.assertEqual(by_mount["/data"].total, 2 * 1024**3)
        # Grand total aggregates across mounts.
        self.assertEqual(result.total, 1024**4 + 2 * 1024**3)


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
        self.assertIn("--max-depth=2", script)
        self.assertIn("scan.sentinel", script)


if __name__ == "__main__":
    unittest.main()
