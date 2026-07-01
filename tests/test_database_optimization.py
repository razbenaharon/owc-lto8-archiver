import os
import sqlite3
import tempfile
import threading
import unittest

from src.db import DatabaseManager, _apply_canonical_remote_paths
from src.maintenance import (
    DatabaseOptimizer,
    HashlessOriginOptimizer,
    _staging_relative,
    _staging_session_id,
)


LEGACY_SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE tapes(tape_id INTEGER PRIMARY KEY,volume_label TEXT UNIQUE NOT NULL,
 date_formatted TEXT,total_capacity INTEGER,used_space INTEGER DEFAULT 0);
CREATE TABLE archive_bundles(bundle_id INTEGER PRIMARY KEY AUTOINCREMENT,
 tape_label TEXT NOT NULL,tape_path TEXT NOT NULL,UNIQUE(tape_label,tape_path));
CREATE TABLE files_index(file_id INTEGER PRIMARY KEY AUTOINCREMENT,file_name TEXT,
 original_path TEXT,file_size_bytes INTEGER,file_hash TEXT,backup_date TEXT,
 tape_label TEXT,is_packed INTEGER,container_name TEXT,stored_path TEXT,
 local_session_id INTEGER,local_chunk_index INTEGER,bundle_id INTEGER,
 file_hash_blob BLOB,record_key BLOB);
CREATE TABLE remote_sessions(session_id INTEGER PRIMARY KEY,session_label TEXT NOT NULL,
 remote_host TEXT NOT NULL,remote_user TEXT NOT NULL,remote_path TEXT NOT NULL,
 tape_label TEXT NOT NULL,staging_dir TEXT NOT NULL,total_files INTEGER,total_bytes INTEGER,
 chunk_count INTEGER,created_at TEXT,completed_at TEXT,status TEXT NOT NULL);
CREATE TABLE remote_manifest(manifest_id INTEGER PRIMARY KEY,session_id INTEGER NOT NULL,
 chunk_index INTEGER NOT NULL,remote_path TEXT NOT NULL,file_name TEXT NOT NULL,
 file_size_bytes INTEGER NOT NULL,local_rel_path TEXT,status TEXT,chunk_status TEXT,
 error_msg TEXT,updated_at TEXT);
"""


class CanonicalPathTests(unittest.TestCase):
    def test_staging_component_is_not_matched_inside_source_path(self):
        self.assertEqual(
            _staging_relative(r"C:\stage\_fetch_s0006_003\project\file.dat"),
            "project/file.dat")
        self.assertIsNone(_staging_relative("/source/_fetch_003/file.dat"))
        self.assertIsNone(_staging_relative(r"C:\source\my_fetch_003\file.dat"))

    def test_staging_session_id_is_extracted_only_from_fetch_component(self):
        self.assertEqual(
            _staging_session_id(r"C:\stage\_fetch_s0042_003\project\file.dat"),
            42)
        self.assertIsNone(_staging_session_id(r"C:\stage\_fetch_003\file.dat"))
        self.assertIsNone(_staging_session_id(r"C:\source\_fetch_s0042_003.dat"))

    def test_metadata_keeps_stored_path_and_gets_canonical_source(self):
        metadata = [{
            'original_path': r'C:\stage\_fetch_000\project\file.dat',
            'stored_path': 'project/file.dat',
        }]
        rows = [{'local_rel_path': 'project/file.dat',
                 'remote_path': '/source/project/file.dat'}]
        self.assertEqual(_apply_canonical_remote_paths(metadata, rows), 1)
        self.assertEqual(metadata[0]['canonical_source_path'],
                         '/source/project/file.dat')
        self.assertEqual(metadata[0]['original_path'], '/source/project/file.dat')
        self.assertEqual(metadata[0]['stored_path'], 'project/file.dat')

    def test_ambiguous_staging_mapping_is_rejected(self):
        with self.assertRaises(RuntimeError):
            _apply_canonical_remote_paths(
                [{'stored_path': 'same.dat'}],
                [{'local_rel_path': 'same.dat', 'remote_path': '/one.dat'},
                 {'local_rel_path': 'same.dat', 'remote_path': '/two.dat'}])


class OptimizerTests(unittest.TestCase):
    def _legacy_database(self, path):
        conn = sqlite3.connect(path)
        conn.executescript(LEGACY_SCHEMA)
        tapes = [('Tape_03',), ('Tape02',), ('Tape01',), ('Tape_01',)]
        conn.executemany('INSERT INTO tapes(volume_label) VALUES (?)', tapes)
        sessions = [
            (11, 'old11', 'Tape02', 'abandoned'),
            (27, 'old27', 'Tape01', 'abandoned'),
            (42, 'kept42', 'Tape_01', 'completed'),
        ]
        for sid, label, tape, status in sessions:
            conn.execute("""INSERT INTO remote_sessions VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (sid,label,'host','user','/strg/E/shared-data',tape,r'C:\stage',
                 2,30,1,'2026-01-01',
                 '2026-01-02' if status == 'completed' else None,status))
        manifest_id = 1
        for sid, _label, tape, status in sessions:
            for rel, size, file_status in (
                    ('project/a.dat',10,'fetched'),
                    ('project/missing.dat',20,'source_missing')):
                conn.execute("""INSERT INTO remote_manifest VALUES
                    (?,?,?,?,?,?,?,?,?,?,?)""",
                    (manifest_id,sid,0,'/strg/E/shared-data/'+rel,
                     os.path.basename(rel),size,
                     rel if file_status == 'fetched' else None,file_status,'done',
                     'missing' if file_status == 'source_missing' else None,
                     '2026-01-02'))
                manifest_id += 1
            # Only fetched files have catalog records in this fixture.
            conn.execute("""INSERT INTO files_index
                (file_name,original_path,file_size_bytes,file_hash,backup_date,tape_label,
                 is_packed,container_name,stored_path)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                ('a.dat',fr'C:\stage\_fetch_s{sid:04d}_000\project\a.dat',10,
                 'ab'*32,'2026-01-02',tape,1,
                 fr'E:\_pack_s{sid:04d}_000\Bundle_001.zip','project/a.dat'))
        conn.execute("""INSERT INTO files_index
            (file_name,original_path,file_size_bytes,file_hash,backup_date,tape_label,
             is_packed,container_name,stored_path)
            VALUES ('legacy.dat',?,5,'','2026-01-01','Tape_03',0,NULL,?)""",
            (r'C:\stage\_fetch_001\legacy\legacy.dat',
             r'E:\_pack_001\legacy\legacy.dat'))
        conn.commit()
        conn.close()

    def test_full_copy_swap_migration(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'archive.db')
            self._legacy_database(path)
            result = DatabaseOptimizer(path, progress=lambda _msg: None).run()
            self.assertEqual(result['validation']['quick_check'], 'ok')
            conn = sqlite3.connect(path)
            conn.row_factory = sqlite3.Row
            paths = {r['tape_label']: (r['original_path'], r['stored_path'])
                     for r in conn.execute(
                         'SELECT tape_label,original_path,stored_path FROM files_index')}
            self.assertEqual(paths['Tape_01'][0],
                             '/strg/E/shared-data/project/a.dat')
            self.assertEqual(paths['Tape_03'][0],
                             '/strg/E/shared-data/legacy/legacy.dat')
            self.assertTrue(paths['Tape_01'][1].endswith('project/a.dat'))
            self.assertEqual(
                [r[0] for r in conn.execute('SELECT session_id FROM remote_sessions')],
                [11, 27, 42])
            self.assertEqual(conn.execute(
                'SELECT COUNT(*) FROM remote_snapshot_files').fetchone()[0], 2)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM remote_file_state WHERE status='source_missing'"
            ).fetchone()[0], 3)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM files_index WHERE source_host='so02'"
            ).fetchone()[0], 4)
            conn.close()

            # The normal API must hydrate normalized names, dates, and origins.
            db = DatabaseManager(path)
            try:
                row = db.search_catalog(tape_label='Tape_01', limit=1)[0]
                self.assertNotIn('file_hash', row)
                self.assertEqual(row['file_name'], 'a.dat')
                self.assertEqual(row['source_host'], 'so02')
                self.assertTrue(row['backup_date'].startswith('2026-01-02'))
            finally:
                db.close()

    def test_ambiguous_legacy_tape_fallback_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'ambiguous.db')
            conn = sqlite3.connect(path)
            conn.executescript(LEGACY_SCHEMA)
            conn.execute("INSERT INTO tapes(volume_label) VALUES ('Tape_A')")
            for sid, remote_path in (
                    (101, '/root/one/same.dat'),
                    (202, '/root/two/same.dat')):
                conn.execute("""INSERT INTO remote_sessions VALUES
                    (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (sid, f's{sid}', 'host', 'user', '/root', 'Tape_A',
                     r'C:\stage', 1, 10, 1, '2026-01-01', None, 'completed'))
                conn.execute("""INSERT INTO remote_manifest VALUES
                    (?,?,?,?,?,?,?,?,?,?,?)""",
                    (sid, sid, 0, remote_path, 'same.dat', 10, 'same.dat',
                     'fetched', 'done', None, '2026-01-01'))
            conn.execute("""INSERT INTO files_index
                (file_name,original_path,file_size_bytes,file_hash,backup_date,
                 tape_label,is_packed,container_name,stored_path)
                VALUES ('same.dat',?,10,'','2026-01-01','Tape_A',0,NULL,?)""",
                (r'C:\stage\_fetch_001\same.dat', r'E:\same.dat'))
            conn.commit()
            conn.close()

            with self.assertRaisesRegex(RuntimeError, 'ambiguous'):
                DatabaseOptimizer(path, source_root='/root',
                                  progress=lambda _msg: None).run()


class SchemaV2RuntimeTests(unittest.TestCase):
    def test_database_manager_serializes_concurrent_public_calls(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = DatabaseManager(os.path.join(tmp, 'worker.db'))
            try:
                db.register_tape('T1', 100)

                def insert_one(idx):
                    db.insert_file(
                        f'{idx}.dat', f'/source/{idx}.dat', idx + 1,
                        'T1', False, None, rf'E:\{idx}.dat')

                threads = [
                    threading.Thread(target=insert_one, args=(idx,))
                    for idx in range(10)
                ]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()

                self.assertEqual(db.count_tape_file_records('T1'), 10)
            finally:
                db.close()

    def test_new_catalog_and_remote_runtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'new.db')
            db = DatabaseManager(path)
            try:
                db.register_tape('T1', 100)
                self.assertTrue(db.insert_file(
                    'file.dat', '/source/file.dat', 10,
                    'T1', True, r'E:\pack\Bundle.zip', 'file.dat',
                    source_host='so01.example.edu'))
                row = db.search_catalog(tape_label='T1')[0]
                self.assertEqual(row['original_path'], '/source/file.dat')
                self.assertNotIn('file_hash', row)
                self.assertEqual(row['source_host'], 'so01')
                self.assertIsNotNone(row['backup_date'])
                self.assertEqual(db.conn.execute(
                    'SELECT COUNT(*) FROM archive_runs').fetchone()[0], 1)

                session_id = db.create_remote_session(
                    'remote-test', 'host', 'user', '/source', 'T1', r'C:\stage')
                db.update_remote_session(
                    session_id, total_files=2, total_bytes=30, chunk_count=1)
                db.insert_remote_manifest_batch(session_id, [
                    (0, '/source/a.dat', 'a.dat', 10),
                    (0, '/source/b.dat', 'b.dat', 20),
                ])
                rows = db.get_chunk_files(session_id, 0)
                self.assertEqual(len(rows), 2)
                db.update_manifest_rows_fetched(
                    [('a.dat', rows[0]['manifest_id'])], session_id=session_id)
                db.update_manifest_row(
                    rows[0]['manifest_id'], session_id=session_id,
                    error_msg='kept note')
                pre_done = db.get_chunk_files(session_id, 0)
                self.assertEqual(pre_done[0]['status'], 'fetched')
                self.assertEqual(pre_done[0]['local_rel_path'], 'a.dat')
                self.assertEqual(pre_done[0]['error_msg'], 'kept note')
                db.update_manifest_row(
                    rows[1]['manifest_id'], session_id=session_id,
                    status='source_missing', error_msg='missing')
                db.update_chunk_status(session_id, 0, 'done')
                states = db.get_chunk_files(session_id, 0)
                self.assertEqual(states[0]['status'], 'fetched')
                self.assertEqual(states[1]['status'], 'source_missing')
            finally:
                db.close()

    def test_bulk_upsert_rejects_unregistered_tape_without_partial_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = DatabaseManager(os.path.join(tmp, 'missing-tape.db'))
            try:
                with self.assertRaises(RuntimeError):
                    db.bulk_upsert_files([{
                        'file_name': 'orphan.dat',
                        'original_path': '/source/orphan.dat',
                        'file_size_bytes': 1,
                        'tape_label': 'MISSING',
                        'is_packed': False,
                        'container_name': None,
                        'stored_path': r'E:\orphan.dat',
                    }])
                self.assertEqual(db.conn.execute(
                    'SELECT COUNT(*) FROM files_index').fetchone()[0], 0)
            finally:
                db.close()

    def test_cleanup_refuses_active_sessions(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = DatabaseManager(os.path.join(tmp, 'active.db'))
            try:
                db.register_tape('T1', 100)
                db.create_remote_session(
                    'active', 'host', 'user', '/source', 'T1', r'C:\stage')
                with self.assertRaises(RuntimeError):
                    db.cleanup_unreferenced_remote_data()
            finally:
                db.close()

    def test_cleanup_removes_only_unreachable_session_storage(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'cleanup.db')
            db = DatabaseManager(path)
            try:
                db.register_tape('T1', 100)
                db.insert_file(
                    'catalog.dat', '/source/catalog.dat', 99,
                    'T1', False, None, r'E:\catalog.dat')

                kept = db.create_remote_session(
                    'kept', 'host', 'user', '/source', 'T1', r'C:\stage')
                db.insert_remote_manifest_batch(
                    kept, [(0, '/source/kept.dat', 'kept.dat', 10)])
                db.update_remote_session(kept, status='completed')

                removed = db.create_remote_session(
                    'removed', 'host', 'user', '/other', 'T1', r'C:\stage')
                db.insert_remote_manifest_batch(
                    removed, [(0, '/other/old.dat', 'old.dat', 20)])
                db.update_remote_session(removed, status='completed')
                db.conn.execute('DELETE FROM remote_sessions WHERE session_id=?',
                                (removed,))
                db.conn.commit()

                before_catalog = db.conn.execute(
                    'SELECT file_id,original_path,file_size_bytes,tape_label '
                    'FROM files_index').fetchall()
                summary = db.get_unreferenced_remote_data_summary()
                self.assertEqual(summary['plans'], 1)
                self.assertEqual(summary['plan_files'], 1)
                self.assertEqual(summary['snapshots'], 1)
                self.assertEqual(summary['snapshot_files'], 1)

                result = db.cleanup_unreferenced_remote_data(compact=True)
                self.assertEqual(result['plans_deleted'], 1)
                self.assertEqual(result['snapshots_deleted'], 1)
                self.assertEqual(result['catalog_files_preserved'], 1)
                self.assertEqual(db.conn.execute(
                    'SELECT COUNT(*) FROM remote_sessions').fetchone()[0], 1)
                self.assertEqual(db.conn.execute(
                    'SELECT COUNT(*) FROM remote_plans').fetchone()[0], 1)
                self.assertEqual(db.conn.execute(
                    'SELECT COUNT(*) FROM remote_snapshots').fetchone()[0], 1)
                after_catalog = db.conn.execute(
                    'SELECT file_id,original_path,file_size_bytes,tape_label '
                    'FROM files_index').fetchall()
                self.assertEqual([tuple(r) for r in before_catalog],
                                 [tuple(r) for r in after_catalog])
            finally:
                db.close()

    def test_delete_session_removes_bookkeeping_not_catalog_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'sessions.db')
            db = DatabaseManager(path)
            try:
                db.register_tape('T1', 100)
                db.insert_file(
                    'catalog.dat', '/source/catalog.dat', 99,
                    'T1', False, None, r'E:\catalog.dat')

                local_id = db.create_local_session(
                    'local', r'C:\source',
                    [[{'name': 'dir', 'size_bytes': 99}]])
                remote_id = db.create_remote_session(
                    'remote', 'host', 'user', '/source', 'T1', r'C:\stage')
                db.insert_remote_manifest_batch(
                    remote_id, [(0, '/source/a.dat', 'a.dat', 10)])

                db.delete_session('local', local_id)
                db.delete_session('remote', remote_id)

                self.assertEqual(db.conn.execute(
                    'SELECT COUNT(*) FROM local_sessions').fetchone()[0], 0)
                self.assertEqual(db.conn.execute(
                    'SELECT COUNT(*) FROM local_chunks_manifest').fetchone()[0], 0)
                self.assertEqual(db.conn.execute(
                    'SELECT COUNT(*) FROM remote_sessions').fetchone()[0], 0)
                self.assertEqual(db.conn.execute(
                    'SELECT COUNT(*) FROM remote_chunks').fetchone()[0], 0)
                self.assertEqual(db.conn.execute(
                    'SELECT COUNT(*) FROM files_index').fetchone()[0], 1)
            finally:
                db.close()

    def test_tape_rename_rolls_back_on_existing_label_conflict(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'rename-rollback.db')
            db = DatabaseManager(path)
            try:
                db.register_tape('OLD', 100)
                db.register_tape('NEW', 100)
                db.insert_file(
                    'same.dat', '/source/same.dat', 1,
                    'OLD', False, None, r'E:\old\same.dat')
                db.insert_file(
                    'same.dat', '/source/same.dat', 1,
                    'NEW', False, None, r'E:\new\same.dat')

                with self.assertRaises(sqlite3.IntegrityError):
                    db.rename_tape('OLD', 'NEW')

                counts = dict(db.conn.execute(
                    'SELECT tape_label,COUNT(*) FROM files_index GROUP BY tape_label'
                ).fetchall())
                self.assertEqual(counts, {'NEW': 1, 'OLD': 1})
                self.assertTrue(db.tape_exists('OLD'))
                self.assertTrue(db.tape_exists('NEW'))
            finally:
                db.close()


class HashlessOriginMigrationTests(unittest.TestCase):
    def test_hashless_origin_migration_drops_hash_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'archive.db')
            OptimizerTests._legacy_database(self, path)
            result = HashlessOriginOptimizer(
                path, progress=lambda _msg: None).run()
            self.assertEqual(result['validation']['quick_check'], 'ok')
            conn = sqlite3.connect(path)
            try:
                columns = {row[1] for row in conn.execute(
                    'PRAGMA table_info(files_index)')}
                self.assertNotIn('file_hash', columns)
                self.assertNotIn('file_hash_blob', columns)
                self.assertIn('source_host', columns)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM files_index WHERE source_host='so02'"
                ).fetchone()[0], 4)
            finally:
                conn.close()


if __name__ == '__main__':
    unittest.main()
