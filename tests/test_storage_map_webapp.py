"""Tests for the storage_map v2 web dashboard (coverage math + API routes)."""
import configparser
import json
import os
import tempfile
import threading
import time
import unittest
from unittest import mock

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

    def test_max_segments_uses_deepest_mount(self):
        self.assertEqual(cov.max_segments(["/strg/D", "/", "/data"], 2), 4)
        self.assertEqual(cov.max_segments(["/"], 2), 2)
        self.assertEqual(cov.max_segments([], 2), 2)


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
    def _build(self, tmp, db_rows, match_depth=2):
        return cov.build_coverage(
            [_scan_result(tmp)],
            db_rows,
            {"so01": ["/strg/D", "/data"]},
            match_depth,
            cov.resolve_host_map(
                [ServerConfig("so01", "so01.iem.technion.ac.il", "u", "",
                              ["/strg/D", "/data"])]),
            db_generated_at="2026-07-07T10:00:00",
        )

    def _rows(self, report, server="so01", mount="/strg/D"):
        srv = next(s for s in report["servers"] if s["name"] == server)
        mt = next(m for m in srv["mounts"] if m["mount"] == mount)
        return {r["path"]: r for r in mt["rows"]}

    def test_statuses_and_cumulative_ancestor_sums(self):
        db_rows = [
            # op fully archived; raw only half; nothing for 'other'.
            _db_row("so01", "/strg/D/shared-data/op", 600 * GIB, files=10,
                    last="2026-06-01T00:00:00"),
            _db_row("so01", "/strg/D/shared-data/raw", 100 * GIB, files=4,
                    last="2026-05-01T00:00:00"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            report = self._build(tmp, db_rows)
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
        # Real percentage is reported, never clamped.
        self.assertAlmostEqual(rows["/strg/D/other"]["coverage_pct"], 200.0)
        self.assertEqual(rows["/strg/D/other"]["status"], "full")

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


class WebAppApiTests(unittest.TestCase):
    def setUp(self):
        try:
            from fastapi.testclient import TestClient
        except ImportError:
            self.skipTest("fastapi is not installed "
                          "(optional storage_map v2 dependency)")
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

    def test_treemap_returns_plotly_figure_json(self):
        resp = self.client.get("/api/treemap")
        self.assertEqual(resp.status_code, 200)
        fig = resp.json()
        self.assertIn("data", fig)
        self.assertEqual(fig["data"][0]["type"], "treemap")

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
            def fetch_coverage_rows(self, max_segs):
                assert max_segs == 4  # /strg/D (2 segs) + match_depth 2
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
        self.assertEqual(op["status"], "full")

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


if __name__ == "__main__":
    unittest.main()
