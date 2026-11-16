"""Strict configuration parsing (audit M9) and example/code key sync.

A yes/no safety setting with a comment on the same line, or a misspelling,
used to be read as "no" without a word; negative, infinite and NaN numbers
were accepted. Those are now startup errors that name section, key and value.
"""
import configparser
import os
import warnings
from unittest import mock

import pytest

from src import config as config_module
from src import ltfs_ownership
from src.cli_errors import OperationalError
from src.config import (CONFIG_SCHEMA, INTENTIONALLY_UNDOCUMENTED,
                        ConfigError, ConfigManager, validate_config)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXAMPLE = os.path.join(REPO_ROOT, "config.example.ini")


def _load(tmp_path, body):
    path = tmp_path / "config.ini"
    path.write_text(body, encoding="utf-8")
    with mock.patch("src.config._load_env_file", return_value={}):
        return ConfigManager(config_path=str(path))


def _error(tmp_path, body):
    with pytest.raises(ConfigError) as caught:
        _load(tmp_path, body)
    return str(caught.value)


# --------------------------------------------------------------------------
# booleans
# --------------------------------------------------------------------------

def test_m9_inline_comment_on_a_safety_switch_is_a_startup_error(tmp_path):
    message = _error(
        tmp_path, "[WINDOWS_UPDATE]\nguard = true ; keep the guard on\n")
    assert "[WINDOWS_UPDATE] guard" in message
    assert "'true ; keep the guard on'" in message
    assert "inline comment" in message.lower()


@pytest.mark.parametrize("section,key,value", [
    ("WINDOWS_UPDATE", "block_on_pending_reboot", "ture"),
    ("WINDOWS_UPDATE", "guard", "yes # default"),
    ("PERFORMANCE", "tape_write_exclusive", "enabled"),
    ("PERFORMANCE", "governor_tape_pause_other_stages", "2"),
    ("REMOTE", "confirm_before_backup", "treu"),
    ("HARDWARE", "eject_after_session", "nope"),
    ("FEATURES", "stored_tar_write_enabled", ""),
    ("TELEGRAM", "enabled", "flase"),
    ("PERFORMANCE", "use_mbuffer", "maybe"),
])
def test_m9_misspelt_boolean_is_a_startup_error(tmp_path, section, key, value):
    message = _error(tmp_path, f"[{section}]\n{key} = {value}\n")
    assert f"[{section}] {key}" in message
    assert repr(value) in message
    assert "true/false/yes/no/on/off/1/0" in message


@pytest.mark.parametrize("text,expected", [
    ("true", True), ("TRUE", True), ("Yes", True), ("on", True), ("1", True),
    ("false", False), ("False", False), ("NO", False), ("Off", False),
    ("0", False), ("  true  ", True),
])
def test_m9_documented_boolean_spellings_are_accepted(tmp_path, text, expected):
    cfg = _load(tmp_path, f"[WINDOWS_UPDATE]\nguard = {text}\n"
                          f"[TELEGRAM]\nenabled = {text}\n"
                          f"[REMOTE]\nconfirm_before_backup = {text}\n"
                          f"[PERFORMANCE]\nuse_mbuffer = {text}\n")
    assert cfg.windows_update_guard is expected
    assert cfg.telegram_enabled is expected
    assert cfg.confirm_before_backup is expected
    assert cfg.use_mbuffer is expected
    assert cfg.warnings == []


def test_m9_getter_never_reads_a_bad_boolean_as_false():
    """A config built without the startup pass must still not guess."""
    cfg = ConfigManager.__new__(ConfigManager)
    cfg.config = configparser.ConfigParser(interpolation=None)
    cfg.config["WINDOWS_UPDATE"] = {"guard": "true ; comment"}
    with pytest.raises(ConfigError):
        cfg.windows_update_guard
    assert cfg.windows_update_block_on_pending_reboot is True   # absent: default


# --------------------------------------------------------------------------
# numbers
# --------------------------------------------------------------------------

@pytest.mark.parametrize("section,key,value", [
    ("PERFORMANCE", "chunk_cap_gb", "-5"),
    ("PERFORMANCE", "chunk_cap_gb", "inf"),
    ("PERFORMANCE", "chunk_cap_gb", "nan"),
    ("PERFORMANCE", "chunk_cap_gb", "0"),
    ("PERFORMANCE", "staging_max_gb", "1e999"),
    ("PERFORMANCE", "staging_max_gb", "-inf"),
    ("PERFORMANCE", "ram_hard_limit_pct", "250"),
    ("PERFORMANCE", "ram_soft_limit_pct", "-1"),
    ("PERFORMANCE", "chunk_max_files", "inf"),
    ("PERFORMANCE", "chunk_max_files", "1.5"),
    ("PERFORMANCE", "fetch_overrun_abort_factor", "-2"),
    ("PERFORMANCE", "staging_padding_factor", "nan"),
    ("PERFORMANCE", "fetch_transient_retry_base_seconds", "soon"),
    ("PERFORMANCE", "ssh_command_timeout_seconds", "inf"),
    ("PIPELINE", "max_ready_chunks", "48   ; chunks"),
    ("PIPELINE", "max_ready_bytes", "-1"),
    ("PIPELINE", "max_write_groups_per_run", "-1"),
    ("REMOTE", "staging_fill_pct", "80"),
    ("SETTINGS", "max_zip_size_gb", "twenty"),
    ("TELEGRAM", "heartbeat_minutes", "nan"),
    ("TELEGRAM", "timeout_seconds", "-10"),
    ("WINDOWS_UPDATE", "pause_days", "0"),
    ("DATABASE", "port", "70000"),
    ("OBSERVATION", "sealed_batch_observation_queue_max", "0"),
    ("HARDWARE", "ltfs_writer_lock_timeout_seconds", "nan"),
])
def test_m9_nonsensical_number_is_a_startup_error(tmp_path, section, key, value):
    message = _error(tmp_path, f"[{section}]\n{key} = {value}\n")
    assert f"[{section}] {key}" in message
    assert repr(value) in message


def test_m9_inline_comment_on_a_number_is_named_as_such(tmp_path):
    message = _error(
        tmp_path, "[PIPELINE]\nmax_ready_bytes = 85899345920   ; 80 GiB\n")
    assert "[PIPELINE] max_ready_bytes" in message
    assert "inline comment" in message.lower()


def test_m9_sensible_numbers_load_and_keep_their_values(tmp_path):
    cfg = _load(tmp_path, "[PERFORMANCE]\nchunk_cap_gb = 12.5\n"
                          "chunk_max_files = 2000\nram_hard_limit_pct = 95\n"
                          "fetch_overrun_abort_factor = 0\n"
                          "[PIPELINE]\nmax_ready_chunks = 7\n"
                          "[REMOTE]\nstaging_fill_pct = 0.5\n")
    assert cfg.chunk_cap_gb == 12.5
    assert cfg.chunk_max_files == 2000
    assert cfg.ram_hard_limit_pct == 95
    assert cfg.fetch_overrun_abort_factor == 0.0
    assert cfg.ready_queue_limits.max_chunks == 7
    assert cfg.staging_fill_pct == 0.5
    assert cfg.warnings == []


def test_m9_choice_values_are_checked(tmp_path):
    message = _error(tmp_path, "[PERFORMANCE]\nrobocopy_priority = hihg\n"
                               "[DATABASE]\nsslmode = required\n")
    assert "[PERFORMANCE] robocopy_priority" in message
    assert "[DATABASE] sslmode" in message
    assert "high" in message and "verify-full" in message


def test_m9_every_problem_is_reported_in_one_error(tmp_path):
    message = _error(tmp_path, "[WINDOWS_UPDATE]\nguard = ture\n"
                               "[PERFORMANCE]\nchunk_cap_gb = nan\n"
                               "tape_write_exclusive = yes ; on\n")
    assert message.count("\n  [") == 3
    assert isinstance(ConfigError("x"), OperationalError)
    assert isinstance(ConfigError("x"), RuntimeError)


# --------------------------------------------------------------------------
# unknown keys / sections: warnings, not errors
# --------------------------------------------------------------------------

def test_m9_unknown_key_in_a_known_section_warns_with_a_suggestion(tmp_path,
                                                                   capsys):
    cfg = _load(tmp_path, "[WINDOWS_UPDATE]\ngaurd = false\n"
                          "block_on_pending_reboot = true\n")
    assert cfg.windows_update_guard is True          # the typo changed nothing
    assert len(cfg.warnings) == 1
    assert "[WINDOWS_UPDATE] gaurd" in cfg.warnings[0]
    assert "guard" in cfg.warnings[0].split("gaurd", 1)[1]
    assert "[WINDOWS_UPDATE] gaurd" in capsys.readouterr().out


def test_m9_unknown_section_and_retired_keys_warn(tmp_path):
    cfg = _load(tmp_path, "[PERFORMANSE]\nchunk_cap_gb = 5\n"
                          "[REMOTE]\nscan_mode = directories\n"
                          "[PERFORMANCE]\npipeline_profile = x\n")
    text = "\n".join(cfg.warnings)
    assert "[PERFORMANSE]" in text and "PERFORMANCE" in text
    assert "[REMOTE] scan_mode" in text and "retired" in text
    assert "[PERFORMANCE] pipeline_profile" in text and "no effect" in text
    assert len(cfg.warnings) == 3


# --------------------------------------------------------------------------
# the shipped example and the generated default
# --------------------------------------------------------------------------

def _every_property(cfg):
    for name in dir(ConfigManager):
        if isinstance(getattr(ConfigManager, name), property):
            getattr(cfg, name)
    cfg.validated_ready_queue_limits()


def test_m9_example_config_loads_with_zero_errors_and_warnings(capsys):
    with warnings.catch_warnings(record=True) as seen, mock.patch(
            "src.config._load_env_file", return_value={}):
        warnings.simplefilter("always")
        cfg = ConfigManager(config_path=EXAMPLE)
        _every_property(cfg)
    assert cfg.warnings == []
    assert [str(item.message) for item in seen] == []
    assert "[CONFIG]" not in capsys.readouterr().out
    # The example's documented values are what the loader actually returns
    # (they used to be swallowed by inline comments and replaced by defaults).
    assert cfg.ready_queue_limits.max_bytes == 85899345920
    assert cfg.windows_update_guard is True
    assert cfg.eject_after_session is False


def test_m9_generated_default_config_loads_cleanly(tmp_path, capsys):
    path = tmp_path / "fresh.ini"
    with mock.patch("src.config._load_env_file", return_value={}):
        cfg = ConfigManager(config_path=str(path))     # creates it
        capsys.readouterr()
        again = ConfigManager(config_path=str(path))   # loads what it wrote
        _every_property(again)
    assert cfg.warnings == [] and again.warnings == []
    assert "WARNING" not in capsys.readouterr().out


def test_m9_example_has_no_inline_comments_or_real_identities():
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(EXAMPLE, encoding="utf-8")
    for section in parser.sections():
        for key, value in parser[section].items():
            if (section, key) == ("PERFORMANCE", "cpu_affinity"):
                continue
            assert " ;" not in value and " #" not in value, (section, key)
    errors, problems = validate_config(parser)
    assert errors == [] and problems == []


# --------------------------------------------------------------------------
# key sync: code <-> schema <-> example
# --------------------------------------------------------------------------

def _keys_read_by_config_module():
    """Every (section, key) src/config.py asks the parser for, by tracing."""
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(EXAMPLE, encoding="utf-8")
    seen = set()
    real_get, real_has = parser.get, parser.has_option

    def get(section, option, **kwargs):
        seen.add((section, option))
        return real_get(section, option, **kwargs)

    def has_option(section, option):
        seen.add((section, option))
        return real_has(section, option)

    parser.get, parser.has_option = get, has_option
    cfg = ConfigManager.__new__(ConfigManager)
    cfg.config = parser
    cfg.env = {}
    with mock.patch.dict(os.environ, {}, clear=False), mock.patch(
            "src.config._incremental_scan_warning_emitted", False):
        for name in ("PGPASSWORD", "REMOTE_PASSWORD", "TELEGRAM_BOT_TOKEN",
                     "TELEGRAM_CHAT_ID"):
            os.environ.pop(name, None)
        _every_property(cfg)
        config_module._warn_ignored_incremental_scan(parser)
    return seen


def _keys_read_by_ltfs_ownership():
    seen = set()

    def record(key):
        seen.add(("HARDWARE", key))
        return ""

    with mock.patch.object(ltfs_ownership, "_read_hardware_option", record):
        ltfs_ownership.configured_ownership_id()
        ltfs_ownership.writer_timeout_seconds()
        ltfs_ownership.helper_timeout_seconds()
    return seen


def _example_keys():
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(EXAMPLE, encoding="utf-8")
    return {(s, k) for s in parser.sections() for k in parser[s]}


def _schema_keys(*statuses):
    return {(section, key)
            for section, options in CONFIG_SCHEMA.items()
            for key, option in options.items()
            if not statuses or option.status in statuses}


def test_every_key_the_code_reads_is_documented_or_allow_listed():
    read = _keys_read_by_config_module()
    undocumented = read - _example_keys()
    assert undocumented == set(INTENTIONALLY_UNDOCUMENTED), (
        "keys read by src/config.py but absent from config.example.ini must "
        "be added to the example or to INTENTIONALLY_UNDOCUMENTED with a "
        f"reason. Unexplained: {sorted(undocumented - set(INTENTIONALLY_UNDOCUMENTED))}; "
        f"stale allow-list entries: {sorted(set(INTENTIONALLY_UNDOCUMENTED) - undocumented)}")
    assert all(reason.strip() for reason in INTENTIONALLY_UNDOCUMENTED.values())


def test_every_key_in_the_example_is_read_by_the_code():
    read = _keys_read_by_config_module() | _keys_read_by_ltfs_ownership()
    dead = _example_keys() - read
    assert dead == set(), f"config.example.ini documents unread keys: {sorted(dead)}"
    # ... and none of them is a key the schema knows to be inert or retired.
    assert _example_keys() <= _schema_keys("live"), sorted(
        _example_keys() - _schema_keys("live"))


def test_schema_matches_what_the_code_reads():
    read = _keys_read_by_config_module() | _keys_read_by_ltfs_ownership()
    assert read <= _schema_keys(), (
        f"read but missing from CONFIG_SCHEMA: {sorted(read - _schema_keys())}")
    unread = _schema_keys("live") - read
    assert unread == set(), f"in CONFIG_SCHEMA but never read: {sorted(unread)}"
    # An inert or retired key has no accessor at all: the schema entry exists
    # only so an old config.ini still loads, with a notice.
    dormant = _schema_keys("inert", "retired") - {("REMOTE", "incremental_scan")}
    assert dormant & read == set(), (
        f"marked inert/retired but still read: {sorted(dormant & read)}")


def test_every_inert_key_still_loads_with_a_no_effect_notice(tmp_path):
    values = {"bool": "true", "int": "5", "float": "5", "str": "x"}
    inert = sorted(_schema_keys("inert"))
    assert len(inert) == 7
    body = {}
    for section, key in inert:
        body.setdefault(section, []).append(
            f"{key} = {values[CONFIG_SCHEMA[section][key].kind]}")
    text = "".join(f"[{section}]\n" + "\n".join(lines) + "\n"
                   for section, lines in body.items())
    cfg = _load(tmp_path, text)
    for section, key in inert:
        assert any(f"[{section}] {key} has no effect" in note
                   for note in cfg.warnings), (section, key)
    assert len(cfg.warnings) == len(inert)


def test_backend_postgres_is_accepted_as_postgresql(tmp_path):
    # 'postgres' is how most people spell it; an existing config.ini with
    # that line must keep starting on the PostgreSQL catalog.
    cfg = _load(tmp_path, "[DATABASE]\nbackend = postgres\n")
    assert cfg.db_backend == "postgresql"
    assert cfg.warnings == []
    message = _error(tmp_path, "[DATABASE]\nbackend = mysql\n")
    assert "[DATABASE] backend" in message


def test_removed_eject_after_pack_is_retired_not_a_typo(tmp_path):
    # As an unknown key it would be "corrected" toward a setting that ejects
    # the cartridge; it must say it was removed and suggest nothing.
    cfg = _load(tmp_path, "[PERFORMANCE]\neject_after_pack = on\n")
    assert len(cfg.warnings) == 1
    assert "retired" in cfg.warnings[0]
    assert "eject_after_session" not in cfg.warnings[0]
    assert cfg.eject_after_session is False
