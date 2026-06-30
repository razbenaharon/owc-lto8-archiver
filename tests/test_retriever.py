import ntpath
import os
import tempfile
import unittest
from unittest import mock

from src.db import DatabaseManager
from src.retriever import LTORetriever


class TapeVerificationTests(unittest.TestCase):
    def test_wrong_tape_then_correct_tape_proceeds(self):
        retriever = LTORetriever(None, r'E:\\', r'C:\stage', r'C:\restore')
        with mock.patch('src.retriever.get_volume_label',
                        side_effect=['WRONG', 'RIGHT']) as labels:
            with mock.patch('builtins.input', return_value='') as prompts:
                retriever._verify_tape('RIGHT')
        self.assertEqual(labels.call_count, 2)
        self.assertEqual(prompts.call_count, 1)

    def test_missing_label_then_correct_tape_proceeds(self):
        retriever = LTORetriever(None, r'E:\\', r'C:\stage', r'C:\restore')
        with mock.patch('src.retriever.get_volume_label',
                        side_effect=[None, 'RIGHT']) as labels:
            with mock.patch('builtins.input', return_value='') as prompts:
                retriever._verify_tape('RIGHT')
        self.assertEqual(labels.call_count, 2)
        self.assertEqual(prompts.call_count, 1)

    def test_cancel_aborts_tape_verification(self):
        retriever = LTORetriever(None, r'E:\\', r'C:\stage', r'C:\restore')
        with mock.patch('src.retriever.get_volume_label', return_value='WRONG'):
            with mock.patch('builtins.input', return_value='CANCEL'):
                with self.assertRaisesRegex(RuntimeError, 'Cancelled'):
                    retriever._verify_tape('RIGHT')


class RetrieverDirectoryTests(unittest.TestCase):
    def test_partial_directory_restore_preserves_source_tree(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, 'archive.db')
            restore_dir = os.path.join(tmp, 'restored')
            db = DatabaseManager(db_path)
            try:
                db.register_tape('Tape_03', 100)
                db.insert_file(
                    'file1.h265',
                    r'C:\temp_for_disk\source\ForTal\DATA_A\file1.h265',
                    123,
                    '',
                    'Tape_03',
                    False,
                    None,
                    r'E:\ForTal\DATA_A\file1.h265')
                db.insert_file(
                    'other.txt',
                    r'C:\temp_for_disk\source\Other\other.txt',
                    1,
                    '',
                    'Tape_03',
                    False,
                    None,
                    r'E:\Other\other.txt')

                rows = db.search_by_directory('ForTal')
                self.assertEqual(len(rows), 1)

                directory_root = LTORetriever._infer_directory_root(
                    'ForTal', rows)
                self.assertEqual(directory_root,
                                 r'C:\temp_for_disk\source\ForTal')

                retriever = LTORetriever(
                    db, r'E:\\', os.path.join(tmp, 'staging'), restore_dir)
                restore_base = ntpath.dirname(directory_root)
                destination = retriever._destination_for_record(
                    rows[0], restore_base=restore_base)

                self.assertEqual(
                    destination,
                    os.path.join(restore_dir, 'ForTal', 'DATA_A',
                                 'file1.h265'))
            finally:
                db.close()


if __name__ == '__main__':
    unittest.main()
