"""Tests for the Storage Map web dashboard (coverage math + API routes)."""
import configparser
import json
import os
import tempfile
import threading
import time
import unittest
from unittest import mock

from storage_map.lib import baseline as baseline_lib
from storage_map.lib.core import ServerConfig, parse_raw_log
from storage_map.webapp import coverage as cov


# A byte-exact (-B1 style) rawlog. The app keeps depth 2 inside shared-data,
# while non-shared-data directories stay at the top layer.
SAMPLE_RAWLOG_B1 = "\n".join([
    "# storage-map raw log",
    "# server: so01.iem.technion.ac.il",
    "# generated_at: 2026-07-01T02:00:00",
    "# depth: 2",
    "##### MOUNT: /strg/D #####",
    "##### DF: 2199023255552 1099511627776 1099511627776 50% #####",
    f"{600 * 1024**3}\t/strg/D/shared-data/op",
    f"{200 * 1024**3}\t/strg/D/shared-data/raw",
    f"{800 * 1024**3}\t/strg/D/shared-data",
    f"{224 * 1024**3}\t/strg/D/other",
    f"{1024 * 1024**3}\t/strg/D",
    "##### MOUNT: /data #####",
    "##### DF: 4294967296 2147483648 2147483648 50% #####",
    f"{2 * 1024**3}\t/data/logs",
    f"{2 * 1024**3}\t/data",
    "##### END #####",
    "",
])

GIB = 1024**3


def _scan_result(tmp):
    path = os.path.join(tmp, "so01_latest.rawlog")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(SAMPLE_RAWLOG_B1)
    return parse_raw_log(path, server_name="so01")


def _db_row(host, prefix, tape_bytes, files=1, last=None):
    return {"host": host, "dir_prefix": prefix, "tape_bytes": tape_bytes,
            "tape_files": files, "last_backup": last}


class CoverageSqlTests(unittest.TestCase):
    def test_sql_filters_posix_rows_and_parameterizes_depth(self):
        self.assertIn("LIKE '/%%'", cov.COVERAGE_SQL)
        self.assertIn("%(max_segs)s", cov.COVERAGE_SQL)
        # Dedup stage groups per file before the prefix rollup.
        self.assertIn("GROUP BY 1, source_host, original_path, 2",
                      cov.COVERAGE_SQL)

    def test_sql_merges_directory_catalog_small_files(self):
        # Small packed files live only in the directory catalog, so coverage
        # must add their totals to files_index...
        self.assertIn("directory_tree_index", cov.COVERAGE_SQL)
        self.assertIn("direct_small_file_bytes", cov.COVERAGE_SQL)
        self.assertIn("%(threshold_bytes)s", cov.COVERAGE_SQL)
        # ...while subtracting the packed-small subset already in files_index
        # (pre-cutover rows + legacy backfill) so nothing double counts.
        self.assertIn("ps_bytes", cov.COVERAGE_SQL)
        self.assertIn("GREATEST(0", cov.COVERAGE_SQL)

    def test_max_segments_uses_deepest_mount(self):
        self.assertEqual(cov.max_segments(["/strg/D", "/", "/data"], 2), 4)
        self.assertEqual(cov.max_segments(["/"], 2), 2)
        self.assertEqual(cov.max_segments([], 2), 2)


class RowStatusTests(unittest.TestCase):
    """The file-count gate: du --inodes counts trump the block-inflated bytes."""

    def test_tiny_file_dir_is_full_when_counts_match_despite_low_byte_ratio(self):
        # 1M sub-block files (~1 KiB each): du allocates a full 4 KiB block per
        # file -> ~4 GiB, while apparent bytes are ~1 GiB (25% byte ratio). The
        # block band [4 - 1M*4KiB - m, 4 + m] still contains 1 GiB, and every
        # file is present, so it is full.
        pct, status = cov._row_status(
            4 * GIB, 1 * GIB, server_files=1_000_000, tape_files=999_500)
        self.assertEqual(status, "full")
        self.assertEqual(pct, 100.0)

    def test_count_mismatch_is_partial_even_with_high_byte_ratio(self):
        # Bytes look 98% complete, but a fifth of the files are missing.
        pct, status = cov._row_status(
            100 * GIB, 98 * GIB, server_files=1_000_000, tape_files=800_000)
        self.assertEqual(status, "partial")
        self.assertLess(pct, 100.0)

    def test_size_below_block_band_is_partial_even_when_counts_match(self):
        # Few big files: the block band is tight ([~10 GiB, ~10 GiB]); only 6 GiB
        # of apparent bytes means content is missing/truncated -> not full.
        _pct, status = cov._row_status(
            10 * GIB, 6 * GIB, server_files=1000, tape_files=1000)
        self.assertEqual(status, "partial")

    def test_size_above_du_is_partial_even_when_counts_match(self):
        # Apparent bytes can't exceed du(allocated) + margin; a big overshoot
        # means duplicated/inflated data, not a clean copy.
        _pct, status = cov._row_status(
            10 * GIB, 100 * GIB, server_files=1000, tape_files=1000)
        self.assertEqual(status, "partial")

    def test_no_counts_is_never_full(self):
        # Byte ratio alone is not accurate enough to certify 'full'.
        self.assertEqual(cov._row_status(100 * GIB, 96 * GIB)[1], "partial")
        self.assertEqual(cov._row_status(100 * GIB, 100 * GIB)[1], "partial")
        self.assertEqual(cov._row_status(100 * GIB, 50 * GIB)[1], "partial")
        self.assertEqual(cov._row_status(100 * GIB, 0)[1], "none")

    def test_source_file_count_prefers_exact_inodes_minus_dirs(self):
        # baseline (exact) > inodes−dirs (exact) > raw inodes (approx).
        self.assertEqual(
            cov._source_file_count(1_000_000, 500, None), (999_500, True))
        self.assertEqual(
            cov._source_file_count(1_000_000, None, None), (1_000_000, False))
        self.assertEqual(
            cov._source_file_count(1_000_000, 500, 990_000), (990_000, True))
        self.assertEqual(cov._source_file_count(None, None, None), (None, False))

    def test_exact_count_catches_large_dir_gap_that_1pct_would_mask(self):
        # APAS-style: a 3M-file dir missing 909 large files. du --inodes alone
        # (1% = 30k slack) would mask it; inodes − dirs gives the exact source
        # count (3,010,000 − 10,877 = 2,999,123), so 909 short reads partial.
        _pct, status = cov._row_status(
            82 * GIB, int(75.3 * GIB), server_files=3_010_000,
            dir_count=10_877, tape_files=2_998_214)
        self.assertEqual(status, "partial")

    def test_exact_count_complete_large_dir_is_full(self):
        # Same dir, backup now holds every file (>= exact source count) and the
        # apparent bytes sit in the block band.
        _pct, status = cov._row_status(
            82 * GIB, int(75.3 * GIB), server_files=3_010_000,
            dir_count=10_877, tape_files=2_999_123)
        self.assertEqual(status, "full")

    def test_exact_baseline_requires_full_count_no_tolerance(self):
        # Exact find baseline: every source file must be present (sub-block
        # files -> du 4 GiB, apparent 1 GiB, all inside the block band).
        self.assertEqual(
            cov._row_status(4 * GIB, 1 * GIB, server_files=1_000_000,
                            tape_files=1_000_000, baseline_files=1_000_000)[1],
            "full")
        # One file short is partial under the baseline, even though the du-inode
        # 1% tolerance would have accepted it.
        self.assertEqual(
            cov._row_status(4 * GIB, 1 * GIB, server_files=1_000_000,
                            tape_files=999_999, baseline_files=1_000_000)[1],
            "partial")

    def test_baseline_takes_priority_over_du_inode_count(self):
        # du --inodes would call this full (within 1%); the exact baseline says
        # 5% of files are missing, so it is partial.
        _pct, status = cov._row_status(
            100 * GIB, 98 * GIB, server_files=1_000_000, tape_files=990_000,
            baseline_files=1_000_000)
        self.assertEqual(status, "partial")


class BaselineTests(unittest.TestCase):
    def test_remote_script_uses_find_type_f(self):
        srv = ServerConfig("so01", "so01.x", "u", "", ["/strg/D"])
        script = baseline_lib._remote_baseline_script(srv, 2)
        self.assertIn("find /strg/D -xdev -type f", script)
        self.assertIn("-printf '%h", script)
        self.assertIn("baseline.sentinel", script)

    def test_parse_counts_ignores_markers(self):
        text = "\n".join([
            "# storage-map exact-count baseline",
            "##### MOUNT: /strg/D #####",
            "1323147\t/strg/D/shared-data/jigsaws",
            "5\t/strg/D",
            "##### END #####",
        ])
        counts = baseline_lib.parse_baseline_counts(text)
        self.assertEqual(counts["/strg/D/shared-data/jigsaws"], 1323147)
        self.assertEqual(counts["/strg/D"], 5)

    def test_write_then_load_round_trips(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "coverage_baseline.json")
            baseline_lib.write_baseline(
                {"so01": {"/strg/D/shared-data/jigsaws": 1323147}}, path)
            loaded = baseline_lib.load_baseline(path)
        self.assertEqual(loaded["so01"]["/strg/D/shared-data/jigsaws"], 1323147)

    def test_load_missing_file_is_empty(self):
        self.assertEqual(baseline_lib.load_baseline("/no/such/baseline.json"), {})


# A rawlog that includes the du --inodes pass. shared-data/op holds ~1M small
# files: du allocates 12 GiB of blocks, apparent bytes are ~10 GiB, and the file
# count matches — inside the block band, so it certifies full.
SAMPLE_RAWLOG_WITH_INODES = "\n".join([
    "# storage-map raw log",
    "# server: so01.iem.technion.ac.il",
    "# generated_at: 2026-07-01T02:00:00",
    "# depth: 2",
    "##### MOUNT: /strg/D #####",
    "##### DF: 2199023255552 1099511627776 1099511627776 50% #####",
    f"{12 * 1024**3}\t/strg/D/shared-data/op",
    f"{12 * 1024**3}\t/strg/D/shared-data",
    f"{12 * 1024**3}\t/strg/D",
    "##### INODES #####",
    "1000000\t/strg/D/shared-data/op",
    "1000010\t/strg/D/shared-data",
    "1000012\t/strg/D",
    "##### END #####",
    "",
])


class InodeParsingTests(unittest.TestCase):
    def test_inode_counts_attach_to_matching_nodes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "so01.rawlog")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(SAMPLE_RAWLOG_WITH_INODES)
            res = parse_raw_log(path, server_name="so01")
        mount = res.mounts[0]
        # Byte size still parsed from the -B1 section (not the inode section).
        self.assertEqual(mount.root.size, 12 * GIB)
        self.assertEqual(mount.root.count, 1000012)
        op = next(c for c in mount.root.children
                  if c.path == "/strg/D/shared-data")
        self.assertEqual(op.count, 1000010)

    def test_missing_inode_section_leaves_counts_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "so01.rawlog")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(SAMPLE_RAWLOG_B1)
            res = parse_raw_log(path, server_name="so01")
        self.assertIsNone(res.mounts[0].root.count)
        self.assertIsNone(res.mounts[0].root.dir_count)

    def test_dirs_section_gives_exact_file_count(self):
        log = "\n".join([
            "# server: so01.x", "# depth: 2",
            "##### MOUNT: /strg/D #####",
            f"{12 * 1024**3}\t/strg/D/shared-data/op",
            f"{12 * 1024**3}\t/strg/D",
            "##### INODES #####",
            "1000000\t/strg/D/shared-data/op", "1000005\t/strg/D",
            "##### DIRS #####",
            "120\t/strg/D/shared-data/op", "130\t/strg/D",
            "##### END #####", "",
        ])
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "so01.rawlog")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(log)
            res = parse_raw_log(path, server_name="so01")
        op = next(c for c in res.mounts[0].root.children
                  if c.path == "/strg/D/shared-data/op")
        self.assertEqual(op.count, 1000000)
        self.assertEqual(op.dir_count, 120)
        # exact recursive file count = inodes − dirs
        self.assertEqual(op.count - op.dir_count, 999880)


class HostMapTests(unittest.TestCase):
    def test_matches_name_and_short_host_case_insensitively(self):
        servers = [
            ServerConfig("so01", "so01.iem.technion.ac.il", "u", "", ["/d"]),
            ServerConfig("lab", "SO02.iem.technion.ac.il", "u", "", ["/d"]),
        ]
        mapping = cov.resolve_host_map(servers)
        self.assertEqual(mapping["so01"], "so01")
        self.assertEqual(mapping["lab"], "lab")
        self.assertEqual(mapping["so02"], "lab")

    def test_override_wins(self):
        servers = [ServerConfig("so01", "so01.example", "u", "", ["/d"])]
        mapping = cov.resolve_host_map(servers, {"OldName": "so01"})
        self.assertEqual(mapping["oldname"], "so01")


class BuildCoverageTests(unittest.TestCase):
    def _build(self, tmp, db_rows, match_depth=2, baseline_by_server=None):
        return cov.build_coverage(
            [_scan_result(tmp)],
            db_rows,
            {"so01": ["/strg/D", "/data"]},
            match_depth,
            cov.resolve_host_map(
                [ServerConfig("so01", "so01.iem.technion.ac.il", "u", "",
                              ["/strg/D", "/data"])]),
            db_generated_at="2026-07-07T10:00:00",
            baseline_by_server=baseline_by_server,
        )

    def _rows(self, report, server="so01", mount="/strg/D"):
        srv = next(s for s in report["servers"] if s["name"] == server)
        mt = next(m for m in srv["mounts"] if m["mount"] == mount)
        return {r["path"]: r for r in mt["rows"]}

    def test_statuses_and_cumulative_ancestor_sums(self):
        db_rows = [
            # op fully archived (exact baseline confirms all 10 files); raw only
            # half; nothing for 'other'.
            _db_row("so01", "/strg/D/shared-data/op", 600 * GIB, files=10,
                    last="2026-06-01T00:00:00"),
            _db_row("so01", "/strg/D/shared-data/raw", 100 * GIB, files=4,
                    last="2026-05-01T00:00:00"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            report = self._build(
                tmp, db_rows,
                baseline_by_server={"so01": {"/strg/D/shared-data/op": 10}})
        rows = self._rows(report)

        op = rows["/strg/D/shared-data/op"]
        self.assertEqual(op["status"], "full")
        self.assertAlmostEqual(op["coverage_pct"], 100.0)
        self.assertEqual(op["depth"], 2)
        self.assertEqual(op["tape_files"], 10)

        raw = rows["/strg/D/shared-data/raw"]
        self.assertEqual(raw["status"], "partial")
        self.assertAlmostEqual(raw["coverage_pct"], 50.0)

        other = rows["/strg/D/other"]
        self.assertEqual(other["status"], "none")
        self.assertEqual(other["tape_bytes"], 0)

        # Ancestors accumulate: shared-data = 600+100, mount root too.
        shared = rows["/strg/D/shared-data"]
        self.assertEqual(shared["tape_bytes"], 700 * GIB)
        self.assertEqual(shared["tape_files"], 14)
        self.assertEqual(shared["last_backup"], "2026-06-01T00:00:00")
        self.assertEqual(shared["depth"], 1)
        root = rows["/strg/D"]
        self.assertEqual(root["tape_bytes"], 700 * GIB)
        self.assertEqual(root["depth"], 0)
        self.assertEqual(root["server_bytes"], 1024 * GIB)

    def test_tape_only_and_over_100_percent(self):
        db_rows = [
            # Directory that no longer exists on the server.
            _db_row("so01", "/strg/D/deleted-proj", 50 * GIB),
            # More bytes on tape than on disk (dir shrank after archive).
            _db_row("so01", "/strg/D/other", 448 * GIB),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            report = self._build(tmp, db_rows)
        rows = self._rows(report)
        self.assertEqual(rows["/strg/D/deleted-proj"]["status"], "tape_only")
        self.assertIsNone(rows["/strg/D/deleted-proj"]["coverage_pct"])
        self.assertIsNone(rows["/strg/D/deleted-proj"]["server_bytes"])
        # More bytes on tape than on disk: without a file count this is not
        # certified full — the byte ratio alone never earns 'full'.
        self.assertEqual(rows["/strg/D/other"]["status"], "partial")

    def test_high_byte_ratio_without_counts_is_not_full(self):
        # 216/224 GiB ~ 96% by bytes, but no file count exists, so it must not
        # be reported full (this is the removed 95%-byte rule).
        with tempfile.TemporaryDirectory() as tmp:
            report = self._build(
                tmp, [_db_row("so01", "/strg/D/other", 216 * GIB)])
        row = self._rows(report)["/strg/D/other"]
        self.assertEqual(row["status"], "partial")
        self.assertIsNone(row["server_files"])

    def test_longest_mount_wins_and_deep_prefixes_roll_up(self):
        db_rows = [
            # 5 segments deep: must roll up into the shared-data child.
            _db_row("so01", "/strg/D/shared-data/op/sub/deep", 10 * GIB),
            # Non-shared-data depth 2 remains top-layer only.
            _db_row("so01", "/strg/D/other/sub", 20 * GIB),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            report = self._build(tmp, db_rows)
        rows = self._rows(report)
        self.assertEqual(rows["/strg/D/shared-data/op"]["tape_bytes"], 10 * GIB)
        self.assertNotIn("/strg/D/shared-data/op/sub", rows)
        self.assertEqual(rows["/strg/D/other"]["tape_bytes"], 20 * GIB)
        self.assertNotIn("/strg/D/other/sub", rows)
        # And it must be under /strg/D, not treated as unmapped.
        srv = report["servers"][0]
        self.assertNotIn(cov.UNMAPPED_MOUNT, [m["mount"] for m in srv["mounts"]])

    def test_unmapped_prefixes_and_unknown_hosts_are_kept_visible(self):
        db_rows = [
            _db_row("so01", "/home/somebody", 1 * GIB),
            _db_row("so99", "/strg/D/x", 2 * GIB),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            report = self._build(tmp, db_rows)
        srv = next(s for s in report["servers"] if s["name"] == "so01")
        unmapped = next(m for m in srv["mounts"]
                        if m["mount"] == cov.UNMAPPED_MOUNT)
        paths = [r["path"] for r in unmapped["rows"]]
        self.assertIn("/home", paths)
        stray = next(s for s in report["servers"]
                     if s["name"] == "so99 (not in config)")
        self.assertFalse(stray["in_config"])

    def test_stale_report_without_db_rows_still_shows_server_side(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = self._build(tmp, [])
        rows = self._rows(report)
        self.assertEqual(rows["/strg/D"]["status"], "none")
        self.assertEqual(rows["/strg/D"]["server_bytes"], 1024 * GIB)

    def test_inode_counts_flow_through_to_full_status(self):
        # The scan carries du --inodes counts; a small-file dir whose apparent
        # bytes sit in the block band and whose count matches reads full e2e.
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "so01_latest.rawlog")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(SAMPLE_RAWLOG_WITH_INODES)
            report = cov.build_coverage(
                [parse_raw_log(path, server_name="so01")],
                [_db_row("so01", "/strg/D/shared-data/op", 10 * GIB,
                         files=1_000_000)],
                {"so01": ["/strg/D"]},
                2,
                cov.resolve_host_map(
                    [ServerConfig("so01", "so01.iem.technion.ac.il", "u", "",
                                  ["/strg/D"])]),
            )
        op = self._rows(report)["/strg/D/shared-data/op"]
        self.assertEqual(op["server_files"], 1_000_000)
        self.assertEqual(op["status"], "full")
        self.assertEqual(op["coverage_pct"], 100.0)

    def test_exact_baseline_overrides_status_end_to_end(self):
        # No du --inodes in this scan (byte-only rawlog); the exact baseline
        # count alone certifies full when apparent bytes match du (600 GiB dir,
        # 598 GiB on tape, all 500k files present).
        with tempfile.TemporaryDirectory() as tmp:
            report = cov.build_coverage(
                [_scan_result(tmp)],
                [_db_row("so01", "/strg/D/shared-data/op", 598 * GIB,
                         files=500_000)],
                {"so01": ["/strg/D"]},
                2,
                cov.resolve_host_map(
                    [ServerConfig("so01", "so01.iem.technion.ac.il", "u", "",
                                  ["/strg/D"])]),
                baseline_by_server={
                    "so01": {"/strg/D/shared-data/op": 500_000}},
            )
        op = self._rows(report)["/strg/D/shared-data/op"]
        self.assertEqual(op["baseline_files"], 500_000)
        self.assertEqual(op["status"], "full")


# --------------------------------------------------------------------------- #
# API tests                                                                    #
# --------------------------------------------------------------------------- #
class _FakeCfg:
    """The minimal ConfigManager surface the web app touches."""

    def __init__(self, tmp):
        self.config = configparser.ConfigParser()
        self.config.read_dict({
            "STORAGE_MAP": {
                "output_dir": os.path.join(tmp, "logs"),
                "dashboard_dir": tmp,
                "scan_depth": "2",
                "mounts": "/strg/D, /data",
                "servers": "so01",
            },
            "STORAGE_MAP:so01": {"host": "so01.iem.technion.ac.il"},
        })
        self.remote_user = "labuser"
        self.remote_password = "sekret-hunter2"
        self.db_dsn = "postgresql://lto@127.0.0.1:5/x"
        self.index_min_file_mb = 10
        self.env = {}


class WebAppApiTests(unittest.TestCase):
    def setUp(self):
        try:
            from fastapi.testclient import TestClient
        except ImportError:
            self.skipTest("fastapi is not installed "
                          "(optional Storage Map dependency)")
        from storage_map.webapp.app import create_app

        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = self._tmp.name
        self.cfg = _FakeCfg(self.tmp)
        os.makedirs(os.path.join(self.tmp, "logs"), exist_ok=True)
        with open(os.path.join(self.tmp, "logs", "so01_latest.rawlog"),
                  "w", encoding="utf-8") as fh:
            fh.write(SAMPLE_RAWLOG_B1)
        self.app = create_app(self.cfg)
        self.client = TestClient(self.app)

    def tearDown(self):
        self._tmp.cleanup()

    def _wait_job(self, name, timeout=5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            job = self.client.get("/api/jobs").json().get(name)
            if job and job["state"] != "running":
                return job
            time.sleep(0.02)
        self.fail(f"job '{name}' did not finish within {timeout}s")

    def test_overview_reports_mounts_and_top_folders(self):
        data = self.client.get("/api/overview").json()
        self.assertEqual(len(data["servers"]), 1)
        srv = data["servers"][0]
        self.assertEqual(srv["name"], "so01")
        self.assertEqual(srv["total"], 1026 * GIB)
        mounts = {m["mount"]: m for m in srv["mounts"]}
        self.assertEqual(mounts["/strg/D"]["free_bytes"], 1024 * GIB)
        self.assertTrue(srv["top_folders"])

    def test_html_export_saves_snapshot_to_docs_dir(self):
        from storage_map.webapp import app as webapp_app

        with tempfile.TemporaryDirectory() as docs_dir:
            with mock.patch.object(webapp_app, "_DOCS_DIR", docs_dir):
                snapshot = "<!doctype html><html><body>snapshot</body></html>"
                response = self.client.post(
                    "/api/export/html", json={"html": snapshot})
                self.assertEqual(response.status_code, 200)
                saved = response.json()
                self.assertTrue(saved["saved"].startswith("storage_map_"))
                self.assertTrue(saved["saved"].endswith(".html"))
                self.assertEqual(os.path.dirname(saved["path"]), docs_dir)
                with open(saved["path"], encoding="utf-8") as fh:
                    self.assertEqual(fh.read(), snapshot)

    def test_html_export_rejects_empty_body(self):
        response = self.client.post("/api/export/html", json={"html": "  "})
        self.assertEqual(response.status_code, 400)

    def test_dashboard_labels_hot_db_directory_totals(self):
        app_js = self.client.get("/static/app.js").text
        self.assertIn("Hot DB size", app_js)
        self.assertIn("Hot DB files", app_js)
        self.assertIn("human(row.tape_bytes || 0)", app_js)
        self.assertIn("Number(row.tape_files || 0).toLocaleString()", app_js)

    def test_scan_status_route_uses_remote_status(self):
        with mock.patch("storage_map.lib.core._remote_status",
                        return_value="DONE"):
            data = self.client.get("/api/scan/status").json()
        self.assertEqual(data["servers"]["so01"]["state"], "DONE")

    def test_scan_job_runs_and_conflicts_return_409(self):
        release = threading.Event()
        started = threading.Event()

        def fake_scan(smcfg, servers, notifier=None):
            started.set()
            release.wait(5)
            return 0

        with mock.patch("storage_map.webapp.app.core.scan",
                        side_effect=fake_scan):
            self.assertEqual(self.client.post("/api/scan", json={}).status_code, 202)
            self.assertTrue(started.wait(5))
            # Same job and its conflicts are refused while running.
            self.assertEqual(self.client.post("/api/scan", json={}).status_code, 409)
            self.assertEqual(self.client.post("/api/fetch", json={}).status_code, 409)
            release.set()
            job = self._wait_job("scan")
        self.assertEqual(job["state"], "done")
        self.assertIn("so01", job["detail"])

    def test_failed_fetch_is_reported_as_failed_job(self):
        with mock.patch("storage_map.webapp.app.core.fetch", return_value=1):
            self.assertEqual(self.client.post("/api/fetch", json={}).status_code, 202)
            job = self._wait_job("fetch")
        self.assertEqual(job["state"], "failed")
        self.assertIn("no completed scans", job["detail"])

    def test_bad_servers_payload_is_rejected(self):
        resp = self.client.post("/api/scan", json={"servers": "so01"})
        self.assertEqual(resp.status_code, 400)
        resp = self.client.post("/api/scan", json={"servers": ["nope"]})
        self.assertEqual(resp.status_code, 400)

    def test_post_without_json_content_type_is_rejected(self):
        # CSRF guard: a cross-origin "simple" POST (no JSON content type)
        # must not be able to trigger SSH-launching actions.
        resp = self.client.post("/api/scan")
        self.assertEqual(resp.status_code, 415)
        resp = self.client.post("/api/scan", content=b"",
                                headers={"Content-Type": "text/plain"})
        self.assertEqual(resp.status_code, 415)

    def test_coverage_refresh_writes_cache_and_coverage_uses_it(self):
        rows = [_db_row("so01", "/strg/D/shared-data/op", 600 * GIB, files=3)]

        class FakeRepo:
            def __init__(self, dsn):
                assert "sekret" not in dsn  # password never in our fake DSN
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def fetch_coverage_rows(self, max_segs, threshold_bytes=None):
                assert max_segs == 4  # /strg/D (2 segs) + match_depth 2
                assert threshold_bytes == 10 * 1024 * 1024  # index_min_file_mb
                return rows

        # Before any refresh the report is stale but still shows du data.
        report = self.client.get("/api/coverage").json()
        self.assertTrue(report["stale"])

        with mock.patch("storage_map.webapp.app.CoverageRepository", FakeRepo):
            self.assertEqual(
                self.client.post("/api/coverage/refresh", json={}).status_code, 202)
            job = self._wait_job("coverage")
        self.assertEqual(job["state"], "done")

        cache_file = os.path.join(self.tmp, "logs", "coverage_cache.json")
        with open(cache_file, encoding="utf-8") as fh:
            cache = json.load(fh)
        self.assertEqual(cache["rows"], rows)

        report = self.client.get("/api/coverage").json()
        self.assertFalse(report["stale"])
        srv = report["servers"][0]
        strg = next(m for m in srv["mounts"] if m["mount"] == "/strg/D")
        op = next(r for r in strg["rows"]
                  if r["path"] == "/strg/D/shared-data/op")
        # Byte-only scan (no du --inodes, no baseline) → not certified full.
        self.assertEqual(op["status"], "partial")

    def test_no_response_ever_contains_the_ssh_password(self):
        with mock.patch("storage_map.lib.core._remote_status",
                        return_value="DONE"):
            bodies = [
                self.client.get("/api/overview").text,
                self.client.get("/api/coverage").text,
                self.client.get("/api/scan/status").text,
                self.client.get("/api/jobs").text,
            ]
        for body in bodies:
            self.assertNotIn("sekret-hunter2", body)


class RunAppTests(unittest.TestCase):
    def test_open_chrome_uses_detected_executable(self):
        from storage_map import run_app

        with mock.patch.object(run_app, "_chrome_path",
                               return_value=r"C:\Chrome\chrome.exe"), \
                mock.patch.object(run_app.subprocess, "Popen") as popen:
            self.assertTrue(run_app._open_chrome("http://127.0.0.1:8765/"))
        popen.assert_called_once_with(
            [r"C:\Chrome\chrome.exe", "http://127.0.0.1:8765/"])


if __name__ == "__main__":
    unittest.main()
