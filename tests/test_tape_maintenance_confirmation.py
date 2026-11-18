"""Audit H4: tape-maintenance eject / filesystem repair need a typed confirmation.

Option 6 of the tape-maintenance submenu ejected immediately and option 4 ran
the LTFS repair tool immediately. In the MAIN menu 6 is "Remote Archive", so a
single keypress in the wrong menu was enough to strand the cartridge outside
the drive (no software "load" exists) or to start a repair on a volume.

No IBM helper ever runs here: subprocess.run and the eject entry point are
replaced, and tests/conftest.py would refuse the executables anyway.
"""
import unittest
from unittest import mock

from src import ltfs
from src.constants import LEGACY_TAPE_PROFILE
from src.ltfs import TapeManager


class _Base(unittest.TestCase):
    def setUp(self):
        self.mgr = TapeManager(mock.MagicMock(), "X:\\", ibm_eject_cmd=None,
                               tape_profile=LEGACY_TAPE_PROFILE)
        self.runs = []
        self.ejects = []
        for target, replacement in (
                ("src.ltfs.subprocess.run", self._fake_run),
                ("src.ltfs.eject_tape_drive", self._fake_eject),
                ("src.ltfs._acquire_tape_io_lock", lambda *a, **k: None),
                ("src.ltfs._release_tape_io_lock", lambda *a, **k: None),
        ):
            patcher = mock.patch(target, replacement)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = mock.patch.object(
            TapeManager, "_print_drive_status", lambda *a, **k: None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _fake_run(self, cmd, *args, **kwargs):
        self.runs.append(list(cmd))
        return mock.Mock(returncode=0, stdout="", stderr="")

    def _fake_eject(self, *args, **kwargs):
        self.ejects.append(args)

    def _answer(self, text, interactive=True):
        """Context: the operator types ``text`` at an interactive console."""
        stack = mock.patch.multiple(
            ltfs, _stdin_is_interactive=lambda: interactive)
        typed = (mock.patch("builtins.input", side_effect=EOFError)
                 if text is None else
                 mock.patch("builtins.input", return_value=text))
        return stack, typed


class EjectNeedsTypedConfirmation(_Base):
    def _eject(self, text, interactive=True):
        stack, typed = self._answer(text, interactive)
        with stack, typed, mock.patch("builtins.print"):
            self.mgr.eject_tape()
        return self.ejects

    def test_enter_cancels(self):
        self.assertEqual(self._eject(""), [])

    def test_a_stray_menu_digit_cancels(self):
        # The exact audit scenario: "6" meant for the main menu.
        self.assertEqual(self._eject("6"), [])
        self.assertEqual(self._eject("y"), [])
        self.assertEqual(self._eject("yes"), [])

    def test_wrong_case_cancels(self):
        self.assertEqual(self._eject("eject"), [])

    def test_closed_stdin_cancels(self):
        self.assertEqual(self._eject(None), [])

    def test_non_interactive_stdin_cancels_even_with_the_right_word(self):
        # A detached run feeding piped menu digits must never eject.
        self.assertEqual(self._eject("EJECT", interactive=False), [])

    def test_typing_the_word_ejects(self):
        self.assertEqual(len(self._eject("EJECT")), 1)

    def test_the_prompt_explains_the_physical_consequence(self):
        stack, typed = self._answer("")
        with stack, typed, mock.patch("builtins.print") as printed:
            self.mgr.eject_tape()
        text = " ".join(str(c.args[0]) for c in printed.call_args_list
                        if c.args).lower()
        self.assertIn("physical", text)
        self.assertIn("cannot be reloaded", text)


class RepairNeedsTypedConfirmation(_Base):
    def _check(self, text, interactive=True):
        stack, typed = self._answer(text, interactive)
        with stack, typed, mock.patch("builtins.print"):
            self.mgr.check_tape()
        return self.runs

    def test_enter_cancels(self):
        self.assertEqual(self._check(""), [])

    def test_a_stray_menu_digit_cancels(self):
        self.assertEqual(self._check("4"), [])
        self.assertEqual(self._check("y"), [])

    def test_closed_stdin_cancels(self):
        self.assertEqual(self._check(None), [])

    def test_non_interactive_stdin_cancels(self):
        self.assertEqual(self._check("REPAIR", interactive=False), [])

    def test_the_eject_word_does_not_authorise_a_repair(self):
        self.assertEqual(self._check("EJECT"), [])

    def test_typing_the_word_runs_the_check(self):
        runs = self._check("REPAIR")
        self.assertEqual(len(runs), 1)
        self.assertIn("LtfsCmdCheck.exe", runs[0][0])


if __name__ == "__main__":
    unittest.main()
