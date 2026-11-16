"""The catalog API returns the same Python types on every backend.

PostgreSQL types every value; SQLite types only declared columns, so the
result of an expression (``MAX(ts)``, ``DATE(ts)``, a computed boolean) comes
back as text or 0/1 unless the read site normalises it. These tests pin the
public read paths so a caller never has to know which engine is behind them.
"""
import unittest
from datetime import date, datetime, timedelta, timezone

from db_backends import create_test_db, each_backend


@each_backend
class ValueTypeParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tdb = create_test_db(cls.BACKEND, "types")
        cls.db = cls.tdb.manager()
        cls.db.register_tape("TY1", 100)
        cls.db.bulk_upsert_files([
            {"original_path": f"/src/dir/file{index}.bin",
             "file_size_bytes": 1000 + index, "tape_label": "TY1",
             "source_host": "srv02", "is_packed": index % 2 == 0,
             "container_name": "Bundle.zip" if index % 2 == 0 else None,
             "stored_path": f"dir/file{index}.bin"}
            for index in range(4)])
        cls.session_id = cls.tdb.run(
            """INSERT INTO remote_sessions (session_label,remote_host,
                   remote_user,remote_path,tape_label,staging_dir,total_files,
                   total_bytes,chunk_count,created_at,status,tape_generation)
               VALUES ('TYPES','h','u','/r','TY1','C:\\stg',1,1,1,now(),
                       'active',1) RETURNING session_id""")[0]["session_id"]
        cls.tdb.run(
            """INSERT INTO archive_runs (run_label,tape_label,session_kind,
                   started_at,completed_at,remote_session_id)
               VALUES ('types:run','TY1','remote',now(),now(),%s)""",
            (cls.session_id,))

    @classmethod
    def tearDownClass(cls):
        cls.tdb.drop()

    def test_timestamp_columns_are_aware_utc_datetimes(self):
        tape = self.db.get_tape("TY1")
        stamp = tape["date_formatted"]
        self.assertIsInstance(stamp, datetime)
        self.assertIsNotNone(stamp.tzinfo)
        self.assertLess(
            abs(datetime.now(timezone.utc) - stamp), timedelta(minutes=5))
        row = self.db.search_catalog(tape_label="TY1")[0]
        self.assertIsInstance(row["catalog_backup_date"], datetime)

    def test_boolean_columns_are_bools_and_bytes_are_bytes(self):
        rows = self.db.search_catalog(tape_label="TY1")
        self.assertEqual({type(row["is_packed"]) for row in rows}, {bool})
        self.assertEqual(sorted(row["is_packed"] for row in rows),
                         [False, False, True, True])
        key = self.tdb.query("SELECT record_key FROM files_index LIMIT 1"
                             )[0]["record_key"]
        self.assertEqual(len(bytes(key)), 32)

    def test_counts_and_sums_are_integers_in_value(self):
        self.assertEqual(self.db.count_tape_file_records("TY1"), 4)
        sessions = self.db.list_backup_sessions()
        self.assertEqual(len(sessions), 1)
        self.assertEqual(int(sessions[0]["file_count"]), 4)
        self.assertEqual(int(sessions[0]["total_bytes"]), 4006)

    def test_a_backup_session_date_is_a_date_and_round_trips(self):
        session = self.db.list_backup_sessions()[0]
        self.assertIs(type(session["session_date"]), date)
        self.assertEqual(
            self.db.count_by_session(session["session_date"], "TY1"), 4)
        self.assertEqual(len(self.db.search_by_session(
            session["session_date"], "TY1")), 4)
        self.assertEqual(len(self.db.search_by_session(
            session["session_date"].isoformat(), "TY1", limit=2)), 2)

    def test_day_filters_are_inclusive_of_the_whole_day(self):
        today = self.db.list_backup_sessions()[0]["session_date"]
        day = timedelta(days=1)
        search = self.db.search_catalog
        self.assertEqual(len(search(date_from=today, date_to=today)), 4)
        self.assertEqual(len(search(date_from=today.isoformat())), 4)
        self.assertEqual(search(date_from=today + day), [])
        self.assertEqual(search(date_to=today - day), [])
        self.assertEqual(len(search(date_to=today + day)), 4)

    def test_aggregated_timestamps_are_datetimes(self):
        footprint = self.db.session_tape_footprint(self.session_id)
        self.assertEqual(len(footprint), 1)
        for key in ("first_run", "last_run"):
            self.assertIsInstance(footprint[0][key], datetime)
            self.assertIsNotNone(footprint[0][key].tzinfo)

    def test_case_insensitive_search_matches_on_both_backends(self):
        self.assertEqual(
            len(self.db.search_catalog(name_query="FILE1.BIN")), 1)
        self.assertEqual(
            len(self.db.search_catalog(name_query="file")), 4)
        # LIKE wildcards in the query are literal text, not patterns.
        self.assertEqual(self.db.search_catalog(name_query="fi%e"), [])
        self.assertEqual(self.db.search_catalog(name_query="file_.bin"), [])


if __name__ == "__main__":
    unittest.main()
