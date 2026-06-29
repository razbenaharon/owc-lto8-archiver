import unittest
from unittest import mock

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


if __name__ == '__main__':
    unittest.main()
