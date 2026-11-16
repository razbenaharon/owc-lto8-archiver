"""A refused tape rename is reported in the menu instead of ending it."""
import io
from contextlib import redirect_stdout
from unittest import mock

from src import cli


class _Db:
    def __init__(self):
        self.renamed = []

    def tape_exists(self, label):
        return label == "T_A"

    def rename_tape(self, old, new):
        raise RuntimeError("FOREIGN KEY constraint failed")


def test_a_refused_rename_keeps_the_menu_running():
    db = _Db()
    answers = iter(["3", "T_A", "T_NEW", "0"])
    out = io.StringIO()
    with mock.patch.object(cli, "_print_tapes_table",
                           return_value=[{"volume_label": "T_A"}]), \
            mock.patch("builtins.input", lambda *_a: next(answers)), \
            redirect_stdout(out):
        cli._db_management_menu(db)       # returns on "0": no exception
    text = out.getvalue()
    assert "Rename refused; nothing was changed" in text
    assert "[OK]" not in text
