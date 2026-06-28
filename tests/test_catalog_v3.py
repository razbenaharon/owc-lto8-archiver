import os
import sqlite3
import tempfile
import unittest

from src.db import DatabaseManager
from src.inspector_repository import InspectorRepository
from src.maintenance import CatalogV3Optimizer, inspect_catalog_database


class CatalogV3Tests(unittest.TestCase):
    def _make_v2_database(self, path):
        db = DatabaseManager(path)
        try:
            db.register_tape('Tape_A', 100)
            db.insert_file(
                'alpha.txt',
                r'C:\source\Project\alpha.txt',
                10,
                'aa' * 32,
                'Tape_A',
                False,
                None,
                r'E:\Project\alpha.txt')
            db.insert_file(
                'beta.bin',
                r'C:\source\Project\Nested\beta.bin',
                20,
                'bb' * 32,
                'Tape_A',
                True,
                r'E:\packs\Bundle.zip',
                'Project/Nested/beta.bin')
        finally:
            db.close()

    def test_preflight_is_read_only_for_v2_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'archive.db')
            self._make_v2_database(path)
            before = os.path.getmtime(path)

            report = inspect_catalog_database(path)

            self.assertFalse(report['catalog_v3_available'])
            self.assertEqual(report['row_counts']['files_index'], 2)
            self.assertEqual(report['quick_check'], 'ok')
            conn = sqlite3.connect(path)
            try:
                self.assertEqual(conn.execute(
                    'PRAGMA user_version').fetchone()[0], 2)
            finally:
                conn.close()
            self.assertEqual(os.path.getmtime(path), before)

    def test_catalog_v3_migration_and_repository_pages(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'archive.db')
            self._make_v2_database(path)

            result = CatalogV3Optimizer(
                path, progress=lambda _msg: None, batch_size=1).run()

            self.assertEqual(result['validation']['quick_check'], 'ok')
            self.assertEqual(result['validation']['files'], 2)
            self.assertEqual(result['validation']['fts_rows'], 2)
            report = inspect_catalog_database(path)
            self.assertTrue(report['catalog_v3_available'])
            self.assertEqual(report['user_version'], 3)

            with InspectorRepository(path) as repo:
                tapes = repo.list_tapes()
                self.assertEqual(tapes[0]['volume_label'], 'Tape_A')

                roots = repo.list_child_directories(
                    tape_label='Tape_A', limit=1)
                self.assertEqual(roots['rows'][0]['name'], 'C:')

                source = repo.list_child_directories(
                    parent_id=roots['rows'][0]['directory_id'])
                self.assertEqual(source['rows'][0]['name'], 'source')

                project = repo.list_child_directories(
                    parent_id=source['rows'][0]['directory_id'])
                project_id = project['rows'][0]['directory_id']
                files = repo.list_directory_files(project_id, limit=1)
                self.assertEqual(files['rows'][0]['file_name'], 'alpha.txt')
                self.assertEqual(files['rows'][0]['file_hash'], 'aa' * 32)
                filtered = repo.list_directory_files(
                    project_id, filters={'name_prefix': 'alp'})
                self.assertEqual(len(filtered['rows']), 1)
                self.assertEqual(filtered['rows'][0]['file_name'], 'alpha.txt')

                search = repo.search_catalog_fts('alpha', limit=5)
                self.assertEqual(len(search['rows']), 1)
                self.assertEqual(search['rows'][0]['file_name'], 'alpha.txt')

    def test_post_v3_inserts_populate_catalog_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'archive.db')
            self._make_v2_database(path)
            CatalogV3Optimizer(path, progress=lambda _msg: None).run()

            db = DatabaseManager(path)
            try:
                self.assertTrue(db.insert_file(
                    'gamma.mov',
                    '/mnt/archive/gamma.mov',
                    30,
                    'cc' * 32,
                    'Tape_A',
                    False,
                    None,
                    'gamma.mov'))
            finally:
                db.close()

            with InspectorRepository(path) as repo:
                search = repo.search_catalog_fts('gamma', limit=5)
                self.assertEqual(len(search['rows']), 1)
                self.assertEqual(search['rows'][0]['original_path'],
                                 '/mnt/archive/gamma.mov')
                file_id = search['rows'][0]['file_id']

            db = DatabaseManager(path)
            try:
                db.delete_file(file_id)
            finally:
                db.close()

            with InspectorRepository(path) as repo:
                self.assertEqual(repo.search_catalog_fts('gamma', limit=5)['rows'], [])

    def test_replace_formatted_tape_clears_catalog_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'archive.db')
            self._make_v2_database(path)
            CatalogV3Optimizer(path, progress=lambda _msg: None).run()

            db = DatabaseManager(path)
            try:
                self.assertTrue(db.replace_formatted_tape(
                    'Tape_A', 123, previous_labels=['Tape_A']))
            finally:
                db.close()

            conn = sqlite3.connect(path)
            try:
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM tapes WHERE volume_label='Tape_A'"
                ).fetchone()[0], 1)
                self.assertEqual(conn.execute(
                    "SELECT used_space FROM tapes WHERE volume_label='Tape_A'"
                ).fetchone()[0], 0)
                self.assertEqual(conn.execute(
                    "SELECT total_capacity FROM tapes WHERE volume_label='Tape_A'"
                ).fetchone()[0], 123)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM files_index WHERE tape_label='Tape_A'"
                ).fetchone()[0], 0)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM archive_bundles WHERE tape_label='Tape_A'"
                ).fetchone()[0], 0)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM archive_runs WHERE tape_label='Tape_A'"
                ).fetchone()[0], 0)
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM catalog_directories WHERE tape_label='Tape_A'"
                ).fetchone()[0], 0)
            finally:
                conn.close()


if __name__ == '__main__':
    unittest.main()
