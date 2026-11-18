"""``[HARDWARE] tape_generation``: the per-cartridge planning budget.

The budget used to be hardcoded for LTO-8. It now comes from a cartridge
generation named in config.ini and is passed explicitly to every caller.
These tests pin the safety contract:

1. no key -> byte-for-byte the legacy LTO-8 budget, label and default;
2. the legacy capacity rule is unchanged across a grid of tape rows;
3. an unknown or malformed generation refuses to start;
4. every generation keeps at least a 4% margin, and no key can remove it;
5. an older cartridge on a newer host is never planned past what it holds;
6. a capacity above the generation's native figure cannot be registered;
7. a resumed local session whose chunks no longer fit refuses to start;
8. the removed module-level budget names are gone for good.
"""
import io
import os
import re
from contextlib import redirect_stdout
from decimal import Decimal
from types import SimpleNamespace
from unittest import mock

import pytest

from src import cli
from src import constants
from src import local_orchestrator as lo
from src import remote_orchestrator as ro
from src.config import CONFIG_SCHEMA, ConfigError, ConfigManager
from src.constants import (LEGACY_TAPE_PROFILE, TAPE_PLAN_HEADROOM,
                           TAPE_PROFILES, registered_budget,
                           registered_capacity_problem, tape_budget_bytes,
                           tape_is_full, tape_profile_banner, tape_usage)
from src.ltfs import TapeManager, prompt_tape_capacity_gb
from src.packer import LTOAnalyzer

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# The figure the archiver planned with before tape_generation existed:
# int(11 TiB * 0.95). Written out so a change to the formula cannot also
# change the expectation.
LEGACY_BUDGET = 11_489_896_510_259

LTO9 = TAPE_PROFILES['lto-9']._replace(configured=True)
LTO8 = TAPE_PROFILES['lto-8']._replace(configured=True)
LTO7 = TAPE_PROFILES['lto-7']._replace(configured=True)


def _cfg(tmp_path, *hardware_lines):
    path = tmp_path / "config.ini"
    path.write_text("\n".join(["[HARDWARE]", *hardware_lines]) + "\n",
                    encoding="utf-8")
    with mock.patch("src.config._load_env_file", return_value={}):
        return ConfigManager(config_path=str(path))


def _old_tape_budget_bytes(total_capacity_gb, used_bytes, reserved_bytes=0,
                           status=None):
    """The function as it was before this change, verbatim in effect."""
    capacity_bytes = LEGACY_BUDGET
    if total_capacity_gb:
        capacity_bytes = min(capacity_bytes,
                             int(float(total_capacity_gb) * 1000**3))
    if tape_is_full(status):
        return capacity_bytes, 0
    return capacity_bytes, max(
        0, capacity_bytes - int(used_bytes) - int(reserved_bytes))


# 1. golden: no key is the legacy profile ------------------------------------

def test_no_key_is_byte_for_byte_the_legacy_profile(tmp_path):
    profile = _cfg(tmp_path, "eject_after_session = false").tape_profile
    assert profile == LEGACY_TAPE_PROFILE
    assert profile.configured is False
    assert profile.budget_bytes == LEGACY_BUDGET == int(11 * 1024**4 * 0.95)
    assert profile.label == "11.5 TB"
    assert profile.register_gb == 12288
    assert tape_profile_banner(profile) == (
        "Tape budget: LTO-8, 11.5 TB per cartridge (default; [HARDWARE] "
        "tape_generation is not set).")


def test_a_config_without_a_hardware_section_is_the_legacy_profile(tmp_path):
    path = tmp_path / "config.ini"
    path.write_text("[SETTINGS]\nzip_threshold_mb = 100\n", encoding="utf-8")
    with mock.patch("src.config._load_env_file", return_value={}):
        assert ConfigManager(config_path=str(path)).tape_profile == \
            LEGACY_TAPE_PROFILE


def test_explicit_lto8_plans_exactly_what_no_key_plans(tmp_path):
    explicit = _cfg(tmp_path, "tape_generation = LTO-8").tape_profile
    assert explicit.configured is True
    assert (explicit.budget_bytes, explicit.label, explicit.register_gb) == (
        LEGACY_BUDGET, "11.5 TB", 12288)
    for used in (0, 10**12, 11 * 10**12, 13 * 10**12):
        assert tape_budget_bytes(12288, used, profile=explicit) == \
            tape_budget_bytes(12288, used, profile=LEGACY_TAPE_PROFILE)


def test_legacy_registration_prompt_is_unchanged():
    asked = []
    answers = iter(["", "45000", "abc"])

    def fake_input(prompt):
        asked.append(prompt)
        return next(answers)

    with mock.patch("builtins.input", fake_input):
        got = [prompt_tape_capacity_gb(LEGACY_TAPE_PROFILE) for _ in range(3)]
    # Enter and junk take the default; the legacy path refuses nothing.
    assert got == [12288, 45000, 12288]
    assert asked[0] == ("Tape capacity in GB (default 12288 for 12 TB, "
                        "Enter to skip): ")


# 2. the legacy capacity rule, across a grid ---------------------------------

@pytest.mark.parametrize("total", [None, 0, 1, 100, 1000, 6000, 12000, 12288,
                                   12288.5, "12288", 18000, 40000])
@pytest.mark.parametrize("used", [0, 1, 10**12, 11_489_896_510_259,
                                  13 * 10**12])
@pytest.mark.parametrize("reserved", [0, 5 * 10**11])
@pytest.mark.parametrize("status", [None, "active", "FULL"])
def test_legacy_profile_matches_the_old_formula(total, used, reserved,
                                                status):
    assert tape_budget_bytes(total, used, reserved, status=status,
                             profile=LEGACY_TAPE_PROFILE) == \
        _old_tape_budget_bytes(total, used, reserved, status)


def test_legacy_usage_percent_is_unchanged():
    usage = tape_usage(12288, int(0.865 * 12288 * 1000**3),
                       profile=LEGACY_TAPE_PROFILE)
    assert usage.budget_bytes == LEGACY_BUDGET
    assert usage.percent_text == "92.5%"


# 3. bad values refuse to start -------------------------------------------------

@pytest.mark.parametrize("value", ["LTO-10", "18", "18TB", "", "LTO-9 ; LTO9",
                                   "lto 9", "LTO-9.5", "12288"])
def test_an_unknown_generation_refuses_to_start(tmp_path, value):
    with pytest.raises(ConfigError, match="tape_generation"):
        _cfg(tmp_path, f"tape_generation = {value}")


@pytest.mark.parametrize("value,generation", [
    ("LTO-9", "LTO-9"), ("lto9", "LTO-9"), (" Lto-9 ", "LTO-9"),
    ("LTO7", "LTO-7"), ("lto-m8", "LTO-M8"), ("LTO-6", "LTO-6")])
def test_known_spellings_are_accepted(tmp_path, value, generation):
    profile = _cfg(tmp_path, f"tape_generation = {value}").tape_profile
    assert (profile.generation, profile.configured) == (generation, True)


def test_the_property_also_refuses_a_bad_value_that_skipped_loading():
    cfg = ConfigManager.__new__(ConfigManager)
    cfg.config = mock.MagicMock()
    cfg.config.get.return_value = "LTO-10"
    with pytest.raises(ConfigError):
        cfg.tape_profile


# 4. the margin cannot be removed ---------------------------------------------

def test_every_generation_keeps_at_least_a_four_percent_margin():
    assert TAPE_PLAN_HEADROOM == 0.95
    for key, profile in TAPE_PROFILES.items():
        assert profile.configured is False, key
        assert 0 < profile.budget_bytes <= 0.96 * profile.native_bytes, key
        assert profile.register_gb * 1000**3 >= profile.budget_bytes, key


def test_no_setting_can_change_the_headroom():
    keys = {key for options in CONFIG_SCHEMA.values() for key in options}
    assert not any("headroom" in key or "budget" in key for key in keys)


def test_generation_budgets():
    assert LTO9.budget_bytes == 17_100_000_000_000
    assert LTO9.label == "17.1 TB"
    assert LTO7.budget_bytes == 5_700_000_000_000
    assert LTO7.label == "5.7 TB"


# 5. older cartridges on a newer host ----------------------------------------

def test_an_lto8_cartridge_on_an_lto9_host_keeps_the_lto8_budget():
    capacity, _ = tape_budget_bytes(12288, 0, profile=LTO9)
    assert capacity == LEGACY_BUDGET
    # The raw registered figure (12.288 TB) is above LTO-8's native 12 TB;
    # it must never be what an LTO-9 host plans with.
    assert capacity < 12 * 1000**4


def test_registered_capacities_get_the_margin_under_a_generation():
    assert tape_budget_bytes(18000, 0, profile=LTO9)[0] == 17_100_000_000_000
    assert tape_budget_bytes(None, 0, profile=LTO9)[0] == 17_100_000_000_000
    assert tape_budget_bytes(12000, 0, profile=LTO9)[0] == 11_400_000_000_000
    assert tape_budget_bytes(6000, 0, profile=LTO9)[0] == 5_700_000_000_000
    assert tape_budget_bytes(12288, 0, profile=LTO8)[0] == LEGACY_BUDGET
    assert registered_budget(12288) == LEGACY_BUDGET
    assert registered_budget(1000) == 950 * 1000**3


@pytest.mark.parametrize("registered", [12288, 12288.0, "12288",
                                        Decimal("12288"), "12288.000"])
def test_the_legacy_registration_matches_whatever_type_the_row_holds(
        registered):
    assert tape_budget_bytes(registered, 0, profile=LTO9)[0] == LEGACY_BUDGET


@pytest.mark.parametrize("registered", [12287, 12287.999, 12288.001, 12289])
def test_a_figure_near_the_legacy_one_never_plans_past_its_own_margin(
        registered):
    capacity, _ = tape_budget_bytes(registered, 0, profile=LTO9)
    assert capacity <= int(float(registered) * 1000**3 * TAPE_PLAN_HEADROOM)


def test_remote_capacity_context_uses_the_configured_profile():
    orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
    orch.cfg = SimpleNamespace(tape_profile=LTO9)
    orch.db = mock.MagicMock()
    orch.db.get_tape.return_value = {"total_capacity": 12288,
                                     "status": "active"}
    orch.db.recalculate_tape_used_space.return_value = 10**12
    with redirect_stdout(io.StringIO()):
        context = orch._remote_tape_capacity_context("Tape_08")
    assert context["capacity_bytes"] == LEGACY_BUDGET
    assert context["available_bytes"] == LEGACY_BUDGET - 10**12


# 6. registration bound -----------------------------------------------------------

def test_a_capacity_above_native_is_refused_only_under_a_generation():
    assert registered_capacity_problem(45000, LTO9) is not None
    assert registered_capacity_problem(18001, LTO9) is not None
    assert registered_capacity_problem(0, LTO9) is not None
    assert registered_capacity_problem(18000, LTO9) is None
    assert registered_capacity_problem(12288, LTO9) is None
    assert registered_capacity_problem(45000, LEGACY_TAPE_PROFILE) is None


@pytest.mark.parametrize("value", [float("nan"), float("inf"),
                                   float("-inf"), -1])
def test_a_non_finite_or_negative_capacity_is_refused(value):
    assert registered_capacity_problem(value, LTO9) is not None


def test_the_prompt_asks_again_and_then_gives_up():
    answers = iter(["45000", "12000"])
    with mock.patch("builtins.input", lambda _p: next(answers)), \
            redirect_stdout(io.StringIO()) as out:
        assert prompt_tape_capacity_gb(LTO9) == 12000
    assert "NATIVE capacity" in out.getvalue()
    with mock.patch("builtins.input", lambda _p: "45000"), \
            redirect_stdout(io.StringIO()):
        assert prompt_tape_capacity_gb(LTO9) is None


def test_manual_registration_registers_nothing_after_refusals():
    db = mock.MagicMock()
    mgr = TapeManager(db, "X:\\", tape_profile=LTO9)
    answers = iter(["Tape_09", "45000", "45000", "45000"])
    with mock.patch("builtins.input", lambda _p: next(answers)), \
            redirect_stdout(io.StringIO()):
        mgr.register_tape()
    db.register_tape.assert_not_called()
    answers = iter(["Tape_09", ""])
    with mock.patch("builtins.input", lambda _p: next(answers)), \
            redirect_stdout(io.StringIO()):
        mgr.register_tape()
    db.register_tape.assert_called_once_with("Tape_09", 18000)


def test_the_database_menu_refuses_a_compressed_capacity():
    db = mock.MagicMock()
    db.tape_exists.return_value = True
    answers = iter(["4", "Tape_09", "45000", "4", "Tape_09", "nan", "0"])
    with mock.patch.object(cli, "_print_tapes_table",
                           return_value=[{"volume_label": "Tape_09"}]), \
            mock.patch("builtins.input", lambda *_a: next(answers)), \
            redirect_stdout(io.StringIO()) as out:
        cli._db_management_menu(db, LTO9)
    db.update_tape_capacity.assert_not_called()
    assert out.getvalue().count("[ERROR]") == 2


# planner -----------------------------------------------------------------------

# A profile with a 950-byte budget, so real files on disk exercise the packer.
TINY = constants.TapeProfile("TINY", 1000, 1000, 1, configured=True)


def _source(tmp_path, sizes):
    for name, size in sizes.items():
        folder = tmp_path / name
        folder.mkdir()
        (folder / "f.bin").write_bytes(b"x" * size)
    return str(tmp_path)


def test_the_planner_packs_against_the_profile_budget(tmp_path):
    source = _source(tmp_path, {"a": 600, "b": 600, "c": 300})
    with redirect_stdout(io.StringIO()) as out:
        analyzer = LTOAnalyzer()
        chunks = analyzer.build_local_allocation_plan(source,
                                                      tape_profile=TINY)
        analyzer.render_allocation_plan(chunks, tape_profile=TINY)
    assert all(sum(e["size_bytes"] for e in c) <= 950 for c in chunks)
    assert len(chunks) == 2
    assert "of 0.0 TB budget" in out.getvalue()


def test_the_planner_refuses_an_entry_over_the_profile_budget(tmp_path):
    source = _source(tmp_path, {"big": 951})
    with redirect_stdout(io.StringIO()), \
            pytest.raises(RuntimeError, match="exceeds"):
        LTOAnalyzer().build_local_allocation_plan(source, tape_profile=TINY)


def test_a_first_tape_budget_above_the_profile_is_clamped(tmp_path):
    source = _source(tmp_path, {"a": 600, "b": 300})
    with redirect_stdout(io.StringIO()):
        chunks = LTOAnalyzer().build_local_allocation_plan(
            source, tape_profile=TINY, first_tape_budget_bytes=10**6)
    assert all(sum(e["size_bytes"] for e in c) <= 950 for c in chunks)


# 7. resume pre-check ---------------------------------------------------------------

def _local_orch(profile, chunk_bytes):
    orch = lo.LocalOrchestrator.__new__(lo.LocalOrchestrator)
    orch.cfg = SimpleNamespace(tape_profile=profile, lto_drive="X:\\")
    orch.db = mock.MagicMock()
    orch.db.get_local_session.return_value = {"session_id": 3,
                                              "total_chunks": 2}
    orch.db.get_local_pending_chunks.return_value = [0, 1]
    orch.db.get_local_chunk_entries.return_value = [
        {"top_level_dir": "projA", "dir_size_bytes": chunk_bytes,
         "tape_label": None}]
    orch._process_chunk = mock.MagicMock(return_value=False)
    return orch


def test_a_resume_with_chunks_over_the_budget_refuses_before_the_drive():
    orch = _local_orch(LTO7, 10 * 1000**4)
    with mock.patch.object(lo, "_ensure_lto_drive_ready") as ready, \
            redirect_stdout(io.StringIO()) as out:
        orch._run_session(3)
    ready.assert_not_called()
    orch._process_chunk.assert_not_called()
    text = out.getvalue()
    assert "REFUSED: 2 pending chunk(s)" in text
    assert "LTO-7 budget of 5.7 TB" in text


def test_a_resume_that_fits_prints_the_budget_banner_and_proceeds():
    orch = _local_orch(LTO7, 5 * 1000**4)
    with mock.patch.object(lo, "_ensure_lto_drive_ready",
                           return_value=True), \
            redirect_stdout(io.StringIO()) as out:
        orch._run_session(3)
    orch._process_chunk.assert_called_once()
    assert ("[TAPE] Tape budget: LTO-7, 5.7 TB per cartridge (from "
            "config.ini [HARDWARE] tape_generation).") in out.getvalue()


# 8. the removed names stay removed -------------------------------------------

REMOVED = ("LOCAL_TAPE_BUDGET_BYTES", "TAPE_BUDGET_LABEL",
           "DEFAULT_TAPE_CAPACITY_GB", "TAPE_PLAN_NOMINAL_BYTES")


def test_the_module_level_budget_names_are_gone():
    for name in REMOVED:
        assert not hasattr(constants, name), name
    pattern = re.compile(r"\b(" + "|".join(REMOVED) + r")\b")
    offenders = []
    roots = [os.path.join(REPO_ROOT, "src"), os.path.join(REPO_ROOT, "scripts")]
    files = [os.path.join(REPO_ROOT, "run.py"),
             os.path.join(REPO_ROOT, "inspect_db.py")]
    for root in roots:
        for current, dirs, names in os.walk(root):
            dirs[:] = [d for d in dirs if d != "__pycache__"]
            files.extend(os.path.join(current, n) for n in names
                         if n.endswith(".py"))
    for path in files:
        with open(path, encoding="utf-8") as handle:
            for number, line in enumerate(handle, 1):
                if pattern.search(line):
                    offenders.append(f"{path}:{number}")
    assert offenders == []
