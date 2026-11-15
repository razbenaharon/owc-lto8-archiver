"""ConfigManager and .env loading."""
import os
import configparser
import difflib
import math
import tempfile
import warnings
from typing import NamedTuple, Optional, Tuple
from urllib.parse import quote

from .cli_errors import OperationalError
from .constants import (BACKUP_LOG_DIR, CONFIG_FILE, LOCAL_STAGING_RESERVE_BYTES,
                        PROJECT_ROOT)
from .paths import _clean_config_path, _clean_remote_path, _config_list

#: Default SQLite catalog file, relative to the project root (gitignored).
DEFAULT_SQLITE_FILENAME = 'lto_archive.sqlite3'


class ConfigError(OperationalError):
    """config.ini holds a value the archiver refuses to guess at.

    An ``OperationalError`` (and so a ``RuntimeError``): both runners already
    print those as one readable message instead of a traceback.
    """


# --- the configuration schema -------------------------------------------------
#
# ONE LINE PER KEY. Adding a setting means adding its line here (and to
# config.example.ini - tests/test_config_strict.py keeps the two in step).
#
# Why this exists (audit M9): values used to be parsed ad hoc at each getter.
# A boolean was "true" only if it matched a short list and "false" for anything
# else, so `guard = true ; keep on` or `guard = ture` silently switched a
# safety guard OFF. Numbers accepted -5, inf and nan. Now every known key is
# checked once at startup against this table, and a bad value stops the run
# with a message naming section, key and value.
#
# Inline comments are NOT supported, by decision: `;` and `#` are legitimate
# inside values here (path lists, cpu_affinity "fetch=0-5;tape=6-7",
# passwords), so stripping them would corrupt real settings. A comment goes on
# its own line; a value with a trailing comment is an error, never a guess.

BOOL_TRUE = ('1', 'true', 'yes', 'on')
BOOL_FALSE = ('0', 'false', 'no', 'off')
BOOL_SPELLINGS = 'true/false/yes/no/on/off/1/0'


class Opt(NamedTuple):
    kind: str                            # bool | int | float | str | choice
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    above: Optional[float] = None        # exclusive lower bound
    choices: Tuple[str, ...] = ()
    # live    - read and acted on
    # inert   - still read by a property, but nothing consumes it
    # retired - no reader at all; accepted so an old config.ini still loads
    status: str = 'live'
    note: str = ''


def _bool():
    return Opt('bool')


def _int(minimum=None, maximum=None):
    return Opt('int', minimum=minimum, maximum=maximum)


def _float(minimum=None, maximum=None, above=None):
    return Opt('float', minimum=minimum, maximum=maximum, above=above)


def _str():
    return Opt('str')


def _choice(*choices):
    return Opt('choice', choices=tuple(choices))


def _inert(option, note):
    return option._replace(status='inert', note=note)


def _retired(note):
    return Opt('str', status='retired', note=note)


_PLAN1 = ("configured the directory-first scanner removed by Plan 1; the "
          "persistent frontier scanner has no such setting")

CONFIG_SCHEMA = {
    'PATHS': {
        'source_dir': _str(),
        'staging_dir': _str(),
        'restore_dir': _str(),
        'backup_log_dir': _str(),
    },
    'DATABASE': {
        'backend': _choice('sqlite', 'postgresql'),
        'sqlite_path': _str(),
        'host': _str(),
        'port': _int(1, 65535),
        'dbname': _str(),
        'user': _str(),
        'password': _str(),
        'sslmode': _choice('disable', 'allow', 'prefer', 'require',
                           'verify-ca', 'verify-full'),
    },
    'HARDWARE': {
        'lto_drive': _str(),
        'ibm_eject_cmd': _str(),
        'eject_after_session': _bool(),
        # Read by src/ltfs_ownership.py (its own minimal reader), not here.
        'ltfs_ownership_id': _str(),
        'ltfs_writer_lock_timeout_seconds': _float(above=0),
        'ltfs_helper_lock_timeout_seconds': _float(above=0),
    },
    'SETTINGS': {
        'zip_threshold_mb': _float(minimum=0),
        'max_zip_size_gb': _float(above=0),
        'stored_tar_max_size_gb': _float(above=0),
    },
    'CATALOG': {
        'index_min_file_mb': _float(minimum=0),
        'index_packed_small_files': _bool(),
        'index_directory_stats': _inert(
            _bool(), "directory stats are always indexed"),
        'index_full_directory_tree': _inert(
            _bool(), "the directory tree is always indexed"),
        'prefer_directory_containers': _inert(
            _bool(), "container choice is made by the packer, not this flag"),
        'small_file_manifest_enabled': _bool(),
        'small_file_manifest_format': _choice('jsonl'),
        'small_file_manifest_compression': _str(),
    },
    'REMOTE': {
        'remote_host': _str(),
        'remote_user': _str(),
        'remote_password': _str(),
        'remote_path': _str(),
        'remote_selected_paths': _str(),
        'confirm_before_backup': _bool(),
        'tape_label': _str(),
        'staging_fill_pct': _float(above=0, maximum=1),
        'large_file_min_mb': _inert(
            _float(minimum=0),
            "no reader in the remote path yet; [CATALOG] index_min_file_mb "
            "is the threshold in force"),
        # Warned about by _warn_ignored_incremental_scan, once per process.
        'incremental_scan': _retired("the frontier scanner is the only one"),
        'scan_mode': _retired(_PLAN1),
        'remote_scan_depth': _retired(_PLAN1),
        'directory_chunk_max_gb': _retired(_PLAN1),
        'directory_chunk_max_files': _retired(_PLAN1),
    },
    'PIPELINE': {
        'min_ready_bytes_before_writer_start': _int(1),
        'target_ready_bytes': _int(1),
        'max_ready_bytes': _int(1),
        'max_ready_chunks': _int(1),
        'ready_queue_staging_reserve_bytes': _int(0),
        'max_unstaged_backlog_chunks': _int(1),
        'max_write_groups_per_run': _int(0),
        'write_only_resume': _bool(),
    },
    'PERFORMANCE': {
        'pipeline_profile': _inert(
            _str(), "there is a single pipeline; no profile is selectable"),
        'chunk_cap_gb': _float(above=0),
        'chunk_max_files': _int(1),
        'prefetch_chunks_ahead': _float(minimum=0),
        'staging_max_gb': _float(above=0),
        'ram_soft_limit_pct': _float(above=0, maximum=100),
        'ram_hard_limit_pct': _float(above=0, maximum=100),
        'fetch_target_mbs': _inert(
            _float(minimum=0), "fetch is not rate-limited by this figure"),
        'fetch_min_free_ram_gb': _float(minimum=0),
        'governor_fetch_target_free_ram_gb': _float(minimum=0),
        'governor_fetch_min_free_floor_gb': _float(minimum=0),
        'governor_fetch_total_ram_cap_pct': _float(above=0, maximum=100),
        'governor_tape_min_free_ram_gb': _float(minimum=0),
        'governor_tape_pause_other_stages': _bool(),
        'governor_tape_exclusive_heavy_stages': _bool(),
        'governor_soft_relax_after_seconds': _float(minimum=0),
        'governor_soft_relax_factor': _float(above=0, maximum=1),
        'governor_status_interval_seconds': _float(minimum=0),
        'governor_memory_sample_interval_seconds': _float(above=0),
        'governor_metadata_batch_size': _int(1),
        'governor_pack_file_batch_size': _int(1),
        'fetch_parallel_streams': _int(1),
        'fetch_transient_retries': _int(0),
        'fetch_transient_retry_base_seconds': _float(minimum=0),
        'fetch_stall_timeout_seconds': _int(0),
        'pack_parallel_workers': _int(1),
        'tape_write_exclusive': _bool(),
        'allow_fetch_during_tape_write': _bool(),
        'allow_pack_during_tape_write': _bool(),
        'allow_db_sync_during_tape_write': _bool(),
        'allow_db_sync_during_fetch': _bool(),
        'allow_pack_during_fetch': _choice('conditional', 'true', 'false'),
        'allow_pack_above_ram_soft': _bool(),
        'allow_resume_oversized_chunks': _bool(),
        'robocopy_priority': _choice('high', 'normal', 'realtime'),
        'cpu_affinity': _str(),
        'ssh_cipher': _str(),
        'ssh_command_timeout_seconds': _float(above=0),
        'use_mbuffer': _bool(),
        'mbuffer_size': _str(),
        'staging_padding_factor': _float(above=0),
        'fetch_overrun_abort_factor': _float(minimum=0),
        'stored_tar_sidecar_fixed_reserve_bytes': _int(0),
        'stored_tar_sidecar_member_reserve_bytes': _int(0),
    },
    'TELEGRAM': {
        'enabled': _bool(),
        'timeout_seconds': _float(above=0),
        'heartbeat_minutes': _float(minimum=0),
        'bot_token': _str(),
        'chat_id': _str(),
    },
    'LOCAL_MANIFEST_ARCHIVE': {
        'root': _str(),
        'search_limit': _inert(
            _int(1), "manifest search takes its limit from the command line"),
    },
    'WINDOWS_UPDATE': {
        'guard': _bool(),
        'pause_days': _int(1),
        'block_on_pending_reboot': _bool(),
    },
    'FEATURES': {
        'stored_tar_write_enabled': _bool(),
        'sealed_tape_write_batches_enabled': _bool(),
        'sealed_tape_write_batches_observation_enabled': _bool(),
    },
    'OBSERVATION': {
        'sealed_batch_observation_queue_max': _int(1),
        'sealed_batch_observation_shutdown_timeout_seconds': _float(minimum=0),
        'sealed_batch_observation_statement_timeout_seconds': _float(minimum=0),
        'sealed_batch_observation_log_path': _str(),
        'sealed_batch_observation_log_max_bytes': _int(1),
        'sealed_batch_observation_log_backups': _int(0),
    },
}

# Keys src/config.py reads that are deliberately NOT in config.example.ini.
# tests/test_config_strict.py fails if a read key is in neither place, or if
# an entry here stops being true - so this list cannot rot.
INTENTIONALLY_UNDOCUMENTED = {
    # Secrets belong in .env (see .env.example); the config.ini fallback
    # exists only so an old file keeps working and must not be advertised.
    ('DATABASE', 'password'): "secret - set PGPASSWORD in .env",
    ('TELEGRAM', 'bot_token'): "secret - set TELEGRAM_BOT_TOKEN in .env",
    ('TELEGRAM', 'chat_id'): "secret - set TELEGRAM_CHAT_ID in .env",
    # Looked for only in order to warn that it does nothing.
    ('REMOTE', 'incremental_scan'): "retired; detected only to warn",
    # Inert: a property still reads them, nothing acts on the value. Offering
    # them in the example would promise an effect that does not exist.
    ('CATALOG', 'index_directory_stats'): "inert - see CONFIG_SCHEMA",
    ('CATALOG', 'index_full_directory_tree'): "inert - see CONFIG_SCHEMA",
    ('CATALOG', 'prefer_directory_containers'): "inert - see CONFIG_SCHEMA",
    ('REMOTE', 'large_file_min_mb'): "inert - see CONFIG_SCHEMA",
    ('PERFORMANCE', 'pipeline_profile'): "inert - see CONFIG_SCHEMA",
    ('PERFORMANCE', 'fetch_target_mbs'): "inert - see CONFIG_SCHEMA",
    ('LOCAL_MANIFEST_ARCHIVE', 'search_limit'): "inert - see CONFIG_SCHEMA",
}


def _describe(section, key, raw, problem):
    text = f"[{section}] {key} = {raw!r}: {problem}"
    if any(mark in str(raw) for mark in (';', '#')):
        text += (" Inline comments are not supported - put the comment on "
                 "its own line.")
    return text


def _parse_number(section, key, raw, option):
    text = str(raw).strip()
    try:
        value = float(text)
    except ValueError:
        raise ConfigError(_describe(section, key, raw, "not a number.")) from None
    if not math.isfinite(value):
        raise ConfigError(_describe(
            section, key, raw, "must be a finite number (not inf/nan)."))
    if option.kind == 'int' and value != int(value):
        raise ConfigError(_describe(section, key, raw, "must be a whole number."))
    if option.above is not None and not value > option.above:
        raise ConfigError(_describe(
            section, key, raw, f"must be greater than {option.above:g}."))
    if option.minimum is not None and value < option.minimum:
        raise ConfigError(_describe(
            section, key, raw, f"must be at least {option.minimum:g}."))
    if option.maximum is not None and value > option.maximum:
        raise ConfigError(_describe(
            section, key, raw, f"must be at most {option.maximum:g}."))
    return int(value) if option.kind == 'int' else value


def _parse_bool(section, key, raw):
    text = str(raw).strip().lower()
    if text in BOOL_TRUE:
        return True
    if text in BOOL_FALSE:
        return False
    raise ConfigError(_describe(
        section, key, raw, f"not a yes/no value. Use one of {BOOL_SPELLINGS}."))


def parse_option(section, key, raw, option):
    """Parse one raw config value against its schema entry, or raise."""
    if option.kind == 'bool':
        return _parse_bool(section, key, raw)
    if option.kind in ('int', 'float'):
        return _parse_number(section, key, raw, option)
    if option.kind == 'choice':
        text = str(raw).strip().lower()
        if text not in option.choices:
            raise ConfigError(_describe(
                section, key, raw,
                f"must be one of {' / '.join(option.choices)}."))
        return text
    return raw


def _option(section, key, kind):
    """The schema entry for a key, or a bare one for a key outside the table."""
    option = CONFIG_SCHEMA.get(section, {}).get(key)
    if option is None or option.kind not in (kind, 'int', 'float'):
        return Opt(kind)
    return option._replace(kind=kind) if kind in ('int', 'float') else option


def _suggest(word, candidates):
    close = difflib.get_close_matches(word, list(candidates), n=1, cutoff=0.6)
    return f" Did you mean {close[0]!r}?" if close else ""


def validate_config(parser):
    """Check a loaded parser against ``CONFIG_SCHEMA``.

    Returns ``(errors, warnings)`` as lists of messages. Errors are values the
    archiver will not run with; warnings are keys that do nothing (unknown,
    most often a typo; inert; retired).
    """
    errors, notes = [], []
    upper = {name.upper(): name for name in CONFIG_SCHEMA}
    for section in parser.sections():
        options = CONFIG_SCHEMA.get(section)
        if options is None:
            same = upper.get(section.upper())
            close = difflib.get_close_matches(
                section.upper(), list(CONFIG_SCHEMA), n=1, cutoff=0.6)
            hint = ""
            if same:
                hint = (f" Did you mean [{same}]? Section names are "
                        "case-sensitive.")
            elif close:
                hint = f" Did you mean [{close[0]}]?"
            notes.append(
                f"[{section}] is not a known section; everything in it is "
                f"ignored.{hint}")
            continue
        for key, raw in parser.items(section, raw=True):
            option = options.get(key)
            if option is None:
                notes.append(
                    f"[{section}] {key} is not a known setting and is "
                    f"ignored.{_suggest(key, options)}")
                continue
            if option.status == 'retired':
                if (section, key) != ('REMOTE', 'incremental_scan'):
                    notes.append(
                        f"[{section}] {key} is retired and ignored "
                        f"({option.note}). Remove it.")
                continue
            try:
                parse_option(section, key, raw, option)
            except ConfigError as exc:
                errors.append(str(exc))
                continue
            if option.status == 'inert':
                notes.append(
                    f"[{section}] {key} has no effect ({option.note}).")
    return errors, notes


_config_warnings_printed = set()


_incremental_scan_warning_emitted = False


def _warn_ignored_incremental_scan(config):
    """Warn once because the retired key must never imply runtime control."""
    global _incremental_scan_warning_emitted
    if (_incremental_scan_warning_emitted
            or not config.has_option('REMOTE', 'incremental_scan')):
        return
    _incremental_scan_warning_emitted = True
    warnings.warn(
        "[REMOTE] incremental_scan is deprecated and ignored; the persistent "
        "frontier scanner is the sole production scanner. Remove this key.",
        UserWarning,
        stacklevel=3,
    )


def _strip_quotes(value):
    """Trim whitespace and one pair of matching surrounding quotes."""
    value = (value or '').strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        value = value[1:-1]
    return value


def _load_env_file(path):
    """Parse a simple KEY=VALUE .env file into a dict.

    Keeps secrets (e.g. REMOTE_PASSWORD) out of the git-tracked config.ini.
    Blank lines and '#' comments are ignored; an optional leading 'export ' and
    surrounding quotes are stripped. Missing/unreadable files yield {}."""
    data = {}
    try:
        with open(path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                if line.startswith('export '):
                    line = line[len('export '):]
                key, val = line.split('=', 1)
                key, val = key.strip(), val.strip()
                if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
                    val = val[1:-1]
                data[key] = val
    except OSError:
        pass
    return data


class ConfigManager:
    def __init__(self, config_path=None):
        # Resolved at call time, not at def time: binding CONFIG_FILE into
        # the signature makes the path impossible to redirect, which is how
        # the test suite ended up creating a real config.ini in the repo
        # root. Production behaviour is unchanged - runners chdir to
        # PROJECT_ROOT and the relative name resolves exactly as before.
        self.config      = configparser.ConfigParser(interpolation=None)
        self.config_path = CONFIG_FILE if config_path is None else config_path
        config_path      = self.config_path

        if not os.path.exists(config_path):
            self._create_default()
            print(f"[CONFIG] Created default config file: {os.path.abspath(config_path)}")
            print("[CONFIG] Please review and edit it before running operations.")

        self.config.read(config_path, encoding='utf-8')
        _warn_ignored_incremental_scan(self.config)

        errors, self.warnings = validate_config(self.config)
        shown = os.path.abspath(config_path)
        for message in self.warnings:
            if (shown, message) not in _config_warnings_printed:
                _config_warnings_printed.add((shown, message))
                print(f"[CONFIG] WARNING: {message}")
        if errors:
            raise ConfigError(
                f"[CONFIG] {shown} has {len(errors)} invalid setting(s); "
                "refusing to start rather than guess:\n  "
                + "\n  ".join(errors))

        # Secrets live in a gitignored .env next to the app, never in config.ini.
        self.env = _load_env_file(os.path.join(PROJECT_ROOT, '.env'))

    def _create_default(self):
        self.config['PATHS'] = {
            'source_dir':  os.path.join(PROJECT_ROOT, 'source'),
            'staging_dir': os.path.join(PROJECT_ROOT, 'staging'),
            'restore_dir': os.path.join(PROJECT_ROOT, 'restored'),
            'backup_log_dir': BACKUP_LOG_DIR,
        }
        # New installs get the zero-setup SQLite catalog. An EXISTING
        # config.ini without a `backend` key keeps PostgreSQL (see
        # db_backend below), so nothing changes for a running system.
        self.config['DATABASE'] = {
            'backend': 'sqlite',
            'sqlite_path': DEFAULT_SQLITE_FILENAME,
        }
        self.config['HARDWARE'] = {
            'lto_drive':     r'D:\\',
            'ibm_eject_cmd': r'C:\Program Files\IBM\LTFS\LtfsCmdEject.exe',
        }
        self.config['SETTINGS'] = {
            'zip_threshold_mb': '100',
            'max_zip_size_gb':  '100',
            'stored_tar_max_size_gb': '100',
        }
        self.config['CATALOG'] = {
            'index_min_file_mb': '10',
            'index_packed_small_files': 'false',
            'small_file_manifest_enabled': 'true',
            'small_file_manifest_format': 'jsonl',
            'small_file_manifest_compression': 'zstd',
        }
        self.config['REMOTE'] = {
            'remote_host':      'your.remote.host',
            'remote_user':      '',
            'remote_password':  '',
            'remote_path':      '',
            'remote_selected_paths': '',
            'confirm_before_backup': 'true',
            'tape_label': '',
            'staging_fill_pct': '0.80',
        }
        self.config['PIPELINE'] = {
            # Byte-bounded ready queue (Phase 4). Values are BYTES and describe
            # files already prepared on the staging disk, never memory.
            'min_ready_bytes_before_writer_start': str(20 * 1024**3),
            'target_ready_bytes':                  str(40 * 1024**3),
            'max_ready_bytes':                     str(80 * 1024**3),
            'max_ready_chunks':                    '48',
            # Phase 4.5. Local staging the ready queue must NEVER consume, so an
            # active fetch + active pack + temp pack output + manifests/resume
            # metadata + the existing reserve always have room. 0 => auto
            # (2x chunk_cap + LOCAL_STAGING_RESERVE_BYTES). Validated against
            # staging_max_gb before a run; a conflict falls back atomically to
            # the documented default limits (never a silent single-value clamp).
            'ready_queue_staging_reserve_bytes':   '0',
        }
        self.config['PERFORMANCE'] = {
            'chunk_cap_gb':          '50',
            'chunk_max_files':       '100000',
            'prefetch_chunks_ahead': '1',
            'staging_max_gb':        '350',
            'ram_soft_limit_pct':    '70',
            'ram_hard_limit_pct':    '85',
            'fetch_min_free_ram_gb': '16',
            'governor_fetch_target_free_ram_gb': '4.0',
            'governor_fetch_min_free_floor_gb': '2.5',
            'governor_tape_min_free_ram_gb': '3.0',
            'governor_tape_pause_other_stages': 'true',
            'governor_tape_exclusive_heavy_stages': 'true',
            'governor_soft_relax_after_seconds': '120',
            'governor_soft_relax_factor': '0.75',
            'governor_status_interval_seconds': '60',
            'governor_memory_sample_interval_seconds': '5',
            'governor_metadata_batch_size': '10000',
            'governor_pack_file_batch_size': '10000',
            'pack_parallel_workers': '1',
            'tape_write_exclusive':  'true',
            'allow_fetch_during_tape_write':   'false',
            'allow_pack_during_tape_write':    'false',
            'allow_db_sync_during_tape_write': 'false',
            'allow_db_sync_during_fetch':      'false',
            'allow_pack_during_fetch':         'conditional',
            'allow_pack_above_ram_soft':       'false',
            'allow_resume_oversized_chunks':   'false',
            'robocopy_priority':     'high',
            'cpu_affinity':          'auto',
            'ssh_cipher':            'aes128-gcm@openssh.com',
            'ssh_command_timeout_seconds': '3600',
            'use_mbuffer':           'false',
            'mbuffer_size':          '512M',
            'staging_padding_factor':      '1.15',
            'fetch_overrun_abort_factor':  '2.0',
            'stored_tar_sidecar_fixed_reserve_bytes': '4096',
            'stored_tar_sidecar_member_reserve_bytes': '1024',
        }
        self.config['TELEGRAM'] = {
            'enabled': 'false',
            'timeout_seconds': '10',
            'heartbeat_minutes': '30',
        }
        self.config['LOCAL_MANIFEST_ARCHIVE'] = {
            'root': os.path.join(PROJECT_ROOT, 'LOCAL_MANIFEST_ARCHIVE'),
        }
        self.config['WINDOWS_UPDATE'] = {
            'guard': 'true',
            'pause_days': '7',
            'block_on_pending_reboot': 'true',
        }
        self.config['FEATURES'] = {
            # Plan 2 Stored TAR creation/initial assignment.  The reader is a
            # separate permanent compatibility path; this flag controls only
            # NEW TAR assignment/production and fails closed when absent or
            # malformed.
            'stored_tar_write_enabled': 'false',
            # Phase 5 sealed tape-write batches. FAIL-CLOSED: default false, and
            # the production runtime queries/mutates no batch table while false.
            # Enabling it without the applied 012 schema must fail at startup.
            'sealed_tape_write_batches_enabled': 'false',
            # Phase 5C observation mode. Default false: no observer is
            # constructed. When true the observer only COMPUTES and logs a shadow
            # comparison; it never influences the writer and its failures cannot
            # block production. Enabling it does NOT enable sealed batches.
            'sealed_tape_write_batches_observation_enabled': 'false',
        }
        self.config['OBSERVATION'] = {
            # Phase 5D bounded async observation (only used when observation mode
            # is enabled). The writer thread only copies a snapshot and does one
            # non-blocking enqueue; this worker does the DB read + observer + log.
            'sealed_batch_observation_queue_max': '100',
            'sealed_batch_observation_shutdown_timeout_seconds': '5',
            'sealed_batch_observation_statement_timeout_seconds': '5',
            # Local diagnostic JSONL. MUST NOT be inside the repo, the LTFS mount,
            # a pack/staging dir, or the evidence tree (validated at build time).
            'sealed_batch_observation_log_path': os.path.join(
                tempfile.gettempdir(), 'lto8_observations',
                'sealed_batch_observations.jsonl'),
            'sealed_batch_observation_log_max_bytes': str(10 * 1024 * 1024),
            'sealed_batch_observation_log_backups': '5',
        }
        with open(self.config_path, 'w', encoding='utf-8') as f:
            self.config.write(f)

    # The getters parse through the same schema as the startup check, so a
    # value is never read two ways. An absent key yields the caller's default;
    # a present but invalid one raises ConfigError - it is never replaced by a
    # default or read as "false" (audit M9). After a successful __init__ no
    # getter can raise: the whole file was already validated.

    def _get_float(self, section, key, fallback):
        raw = self.config.get(section, key, fallback=None)
        if raw is None:
            return float(fallback)
        return float(parse_option(section, key, raw,
                                  _option(section, key, 'float')))

    def _get_bool(self, section, key, fallback):
        raw = self.config.get(section, key, fallback=None)
        if raw is None:
            return bool(fallback)
        return _parse_bool(section, key, raw)

    def _get_int(self, section, key, fallback, minimum=None):
        raw = self.config.get(section, key, fallback=None)
        if raw is None:
            value = int(fallback)
        else:
            value = int(parse_option(section, key, raw,
                                     _option(section, key, 'int')))
        if minimum is not None:
            value = max(minimum, value)
        return value

    def _get_choice(self, section, key, fallback):
        raw = self.config.get(section, key, fallback=None)
        if raw is None:
            return fallback
        option = CONFIG_SCHEMA[section][key]
        return parse_option(section, key, raw, option)

    @property
    def source_dir(self):
        return _clean_config_path(self.config.get(
            'PATHS', 'source_dir',
            fallback=os.path.join(PROJECT_ROOT, 'source')))
    @property
    def staging_dir(self):
        return _clean_config_path(self.config.get(
            'PATHS', 'staging_dir',
            fallback=os.path.join(PROJECT_ROOT, 'staging')))
    @property
    def restore_dir(self):
        return _clean_config_path(self.config.get(
            'PATHS', 'restore_dir',
            fallback=os.path.join(PROJECT_ROOT, 'restored')))
    @property
    def pg_host(self):
        return self.config.get('DATABASE', 'host', fallback='localhost').strip()
    @property
    def pg_port(self):
        return self.config.get('DATABASE', 'port', fallback='5432').strip()
    @property
    def pg_dbname(self):
        return self.config.get('DATABASE', 'dbname', fallback='lto_archive').strip()
    @property
    def pg_user(self):
        return self.config.get('DATABASE', 'user', fallback='lto').strip()
    @property
    def pg_password(self):
        return _strip_quotes(
            os.environ.get('PGPASSWORD')
            or self.env.get('PGPASSWORD')
            or self.config.get('DATABASE', 'password', fallback='', raw=True))
    @property
    def pg_sslmode(self):
        return self.config.get('DATABASE', 'sslmode', fallback='prefer').strip()
    @property
    def db_backend(self):
        """Catalog backend: ``'sqlite'`` or ``'postgresql'``.

        A config with no ``backend`` key is an install that predates the
        SQLite backend, so it stays on PostgreSQL. Only an explicit
        ``backend = sqlite`` (which the generated default config and
        config.example.ini carry) selects SQLite.
        """
        return self._get_choice('DATABASE', 'backend', 'postgresql')
    @property
    def sqlite_path(self):
        """Absolute path of the SQLite catalog file (project root by default)."""
        raw = _clean_config_path(self.config.get(
            'DATABASE', 'sqlite_path', fallback=DEFAULT_SQLITE_FILENAME))
        if not raw:
            raw = DEFAULT_SQLITE_FILENAME
        if not os.path.isabs(raw):
            raw = os.path.join(PROJECT_ROOT, raw)
        return os.path.abspath(raw)
    @property
    def db_dsn(self):
        if self.db_backend == 'sqlite':
            return 'sqlite:///' + self.sqlite_path.replace('\\', '/')
        user = quote(self.pg_user, safe='')
        password = quote(self.pg_password, safe='')
        auth = f"{user}:{password}@" if password else f"{user}@"
        return (
            f"postgresql://{auth}{self.pg_host}:{self.pg_port}/"
            f"{quote(self.pg_dbname, safe='')}?sslmode={quote(self.pg_sslmode, safe='')}"
        )
    @property
    def db_display_ref(self):
        if self.db_backend == 'sqlite':
            return 'sqlite:///' + self.sqlite_path.replace('\\', '/')
        user = quote(self.pg_user, safe='')
        auth = f"{user}:***@" if self.pg_password else f"{user}@"
        return (
            f"postgresql://{auth}{self.pg_host}:{self.pg_port}/"
            f"{quote(self.pg_dbname, safe='')}?sslmode={quote(self.pg_sslmode, safe='')}"
        )
    @property
    def backup_log_dir(self):
        return _clean_config_path(self.config.get('PATHS', 'backup_log_dir',
                                                  fallback=BACKUP_LOG_DIR))
    @property
    def lto_drive(self):
        return _clean_config_path(self.config.get(
            'HARDWARE', 'lto_drive', fallback=r'D:\\'))
    @property
    def ibm_eject_cmd(self):
        return _clean_config_path(self.config.get(
            'HARDWARE', 'ibm_eject_cmd',
            fallback=r'C:\Program Files\IBM\LTFS\LtfsCmdEject.exe'))
    @property
    def eject_after_session(self):
        """Whether a completed remote session physically ejects the cartridge.

        Defaults to **False**, which is what the no-physical-intervention policy
        has required all along: `LtfsCmdEject` is mechanical, and a cartridge
        ejected with nobody at the drive cannot be reloaded remotely — there is
        no software "load" for a tape sitting out of the slot. A run that
        finishes overnight would strand the drive until someone travels to it.

        The remote orchestrator ejected unconditionally after the last chunk,
        which contradicted that policy; this flag makes the safe behaviour the
        default and keeps the eject available for an operator who is standing
        at the drive and wants the tape out.
        """
        return self._get_bool('HARDWARE', 'eject_after_session', False)
    @property
    def zip_threshold_mb(self):
        return self._get_float('SETTINGS', 'zip_threshold_mb', 100)
    @property
    def max_zip_size_gb(self):
        return self._get_float('SETTINGS', 'max_zip_size_gb', 100)
    @property
    def stored_tar_max_size_gb(self):
        return self._get_float(
            'SETTINGS', 'stored_tar_max_size_gb', self.max_zip_size_gb)
    @property
    def index_min_file_mb(self):
        return self._get_float('CATALOG', 'index_min_file_mb', 10)
    @property
    def index_packed_small_files(self):
        return self._get_bool('CATALOG', 'index_packed_small_files', False)
    @property
    def index_directory_stats(self):
        return self._get_bool('CATALOG', 'index_directory_stats', True)
    @property
    def index_full_directory_tree(self):
        return self._get_bool('CATALOG', 'index_full_directory_tree', True)
    @property
    def prefer_directory_containers(self):
        return self._get_bool('CATALOG', 'prefer_directory_containers', True)
    @property
    def small_file_manifest_enabled(self):
        return self._get_bool('CATALOG', 'small_file_manifest_enabled', True)
    @property
    def small_file_manifest_format(self):
        return self._get_choice(
            'CATALOG', 'small_file_manifest_format', 'jsonl')
    @property
    def small_file_manifest_compression(self):
        return self.config.get(
            'CATALOG', 'small_file_manifest_compression',
            fallback='zstd').strip().lower()
    @property
    def remote_host(self):      return self.config.get('REMOTE', 'remote_host', fallback='')
    @property
    def remote_user(self):      return self.config.get('REMOTE', 'remote_user', fallback='')
    @property
    def remote_password(self):
        # Priority: process env var > .env file > config.ini (kept empty in git).
        return _strip_quotes(
            os.environ.get('REMOTE_PASSWORD')
            or self.env.get('REMOTE_PASSWORD')
            or self.config.get('REMOTE', 'remote_password', fallback='', raw=True))
    @property
    def remote_path(self):      return _clean_remote_path(self.config.get('REMOTE', 'remote_path', fallback=''))
    @property
    def remote_selected_paths(self):
        paths = [_clean_remote_path(p)
                 for p in _config_list(self.config.get('REMOTE', 'remote_selected_paths', fallback='', raw=True))]
        return [p for p in paths if p]
    @property
    def remote_scan_paths(self):
        return self.remote_selected_paths or ([self.remote_path] if self.remote_path else [])
    @property
    def confirm_before_backup(self):
        return self._get_bool('REMOTE', 'confirm_before_backup', True)
    @property
    def staging_fill_pct(self):
        return self._get_float('REMOTE', 'staging_fill_pct', 0.80)
    # ``scan_mode``, ``remote_scan_depth``, ``directory_chunk_max_gb`` and
    # ``directory_chunk_max_files`` were removed by Plan 1 Task 1.6: the audit
    # found no reader for any of them — they configured the dormant
    # directory-first scanner that was never wired into the production
    # new-session path. An existing config.ini may still contain the keys;
    # configparser ignores keys nothing asks for, so no operator action is
    # needed. Plan 1 Task 2.3's directory frontier introduces its own settings.
    @property
    def large_file_min_mb(self):
        """Threshold above which a file is archived loose rather than packed.

        Retained (unlike its directory-first siblings) because it is a real
        archival policy figure, defaulting to the catalog's own
        ``index_min_file_mb``, and Plans 2-3 size loose/stored-TAR behaviour on
        it. It currently has no reader in the remote path.
        """
        return self._get_float('REMOTE', 'large_file_min_mb',
                               self.index_min_file_mb)
    @property
    def remote_tape_label(self):
        """The cartridge a NEW remote session should target (Plan 1 Task 1.4).

        Blank by default. It exists so a fresh session can name its target
        **without reading the drive**: the old flow auto-detected the label by
        reading the mounted volume before anything else happened, which is a
        device access outside a finite write group — exactly what Task 1.4
        removes. When this is blank an interactive run prompts for the label;
        a headless run without ``--resume`` still refuses to start, blank or
        not, because a fresh session also needs the capacity confirmation.

        A RESUMED session ignores this entirely and uses its persisted
        label/generation, so editing config.ini can never re-point work that is
        already planned against a cartridge.
        """
        return (self.config.get('REMOTE', 'tape_label', fallback='') or '').strip()

    # --- [PERFORMANCE] : continuous-streaming pipeline tuning -----------------
    @property
    def pipeline_profile(self):
        return self.config.get(
            'PERFORMANCE', 'pipeline_profile',
            fallback='tape_first_controlled').strip()
    @property
    def chunk_cap_gb(self):
        return self._get_float('PERFORMANCE', 'chunk_cap_gb', 50)
    @property
    def chunk_max_files(self):
        return self._get_int('PERFORMANCE', 'chunk_max_files', 100000,
                             minimum=1)
    # -- Phase 4 byte-bounded ready queue --------------------------------
    _READY_QUEUE_DEFAULTS = {
        'min_ready_bytes_before_writer_start': 20 * 1024**3,
        'target_ready_bytes': 40 * 1024**3,
        'max_ready_bytes': 80 * 1024**3,
        'max_ready_chunks': 48,
    }

    def _ready_queue_raw(self, key):
        return self._get_int('PIPELINE', key, self._READY_QUEUE_DEFAULTS[key],
                             minimum=1)

    @property
    def ready_queue_limits(self):
        """Validated :class:`~src.ready_queue.ReadyQueueLimits`.

        Malformed individual values already degrade to the documented default
        via ``_get_int``. If the *combination* is still inconsistent (e.g. min >
        target), fall back to the full default set rather than running with an
        ordering that would make the writer-start rules nonsensical — and say so
        loudly, because a silently wrong threshold is how the drive ends up
        idle again.
        """
        from .ready_queue import ReadyQueueLimits
        values = {k: self._ready_queue_raw(k) for k in self._READY_QUEUE_DEFAULTS}
        try:
            return ReadyQueueLimits(
                values['min_ready_bytes_before_writer_start'],
                values['target_ready_bytes'],
                values['max_ready_bytes'],
                values['max_ready_chunks'])
        except ValueError as e:
            print(f"[CONFIG] Invalid [PIPELINE] ready-queue limits ({e}); "
                  f"using defaults {self._READY_QUEUE_DEFAULTS}.")
            d = self._READY_QUEUE_DEFAULTS
            return ReadyQueueLimits(
                d['min_ready_bytes_before_writer_start'],
                d['target_ready_bytes'], d['max_ready_bytes'],
                d['max_ready_chunks'])

    @property
    def ready_queue_staging_reserve_bytes(self):
        """Local staging the ready queue must never consume (Phase 4.5).

        ``0`` (the default) means *auto*: enough for one active fetch plus one
        active pack (each up to ``chunk_cap``, which is the peak the capacity
        gate already sizes on) plus the existing ``LOCAL_STAGING_RESERVE_BYTES``
        headroom that also covers temp pack output, manifests and resume
        metadata. This deliberately mirrors the ``need = 2 * physical_estimate``
        figure in ``_await_staging_capacity`` so there is a single staging model,
        not a second incompatible one."""
        auto = 2 * int(self.chunk_cap_gb * 1024**3) + LOCAL_STAGING_RESERVE_BYTES
        raw = self._get_int('PIPELINE', 'ready_queue_staging_reserve_bytes', 0,
                            minimum=0)
        return auto if raw == 0 else raw

    @property
    def max_unstaged_backlog_chunks(self):
        """How far the scanner may run ahead of the stager (Plan 1 Task 1.3).

        A **bound**, not a barrier. The pipeline re-derives the number of sealed
        chunks that no worker has started staging from authoritative chunk
        status on every scheduling decision, and pauses publication while that
        number is at this limit. As soon as the stager takes one chunk,
        exploration resumes — which is what stops a large resumed backlog from
        indefinitely postponing renewed scanning, the failure mode the old
        bounded hand-off queue had.

        Raising it costs only planning rows; lowering it below a handful will
        make the scanner wait on the (much slower) fetch/pack stage.
        """
        return self._get_int('PIPELINE', 'max_unstaged_backlog_chunks', 64,
                             minimum=1)

    @property
    def max_write_groups_per_run(self):
        """Stop a run after this many committed write groups. 0 = unbounded.

        The runbook requires that the next finite group never starts by
        itself, and there was no way to express that: a run drained every
        ready chunk. Setting this to 1 makes a bounded group self-terminating,
        which is safer than letting it continue and stopping it by hand -- the
        limit is checked AFTER a group commits and BEFORE the next is
        selected, so the run always ends on a chunk boundary with the LTFS
        index synced and the session resumable.
        """
        return self._get_int('PIPELINE', 'max_write_groups_per_run', 0,
                             minimum=0)

    @property
    def write_only_resume(self):
        """Drain already-planned sealed chunks; run no scanner at all.

        Writing a chunk that was planned and sealed by an earlier run needs no
        discovery, but the resume path always started the frontier scanner
        alongside the stager, so a healthy write depended on traversal being
        healthy too. On a pre-frontier session it is not: the resumed scanner
        collides on ``remote_scan_directories`` traversal ordinals.

        With this set the orchestrator does not construct a scan coordinator,
        so nothing can traverse, publish a scan segment, mutate the frontier or
        create a chunk. The stager additionally admits only chunks whose
        durable state is already ``membership_state='sealed'`` and
        ``plan_source='legacy_db'``. Every tape, generation, ownership, lease,
        capacity, writer-ambiguity and crash-recovery gate is unchanged.

        Default off: a normal session must keep scanning.
        """
        return self._get_bool('PIPELINE', 'write_only_resume', False)

    def validated_ready_queue_limits(self):
        """Ready-queue limits proven to leave room for fetch/pack (Phase 4.5).

        The Phase-4 ``ready_queue_limits`` already validate internal ordering.
        Here the *combination* with staging is checked: the queue's byte ceiling
        plus the staging reserve must fit inside the effective staging budget, so
        a large ``max_ready_bytes`` can never starve the producer's active
        fetch+pack. On a conflict we fall back ATOMICALLY to the documented
        default limit set (never clamp one value alone); if even the defaults
        cannot fit, we fail closed so the run refuses to start rather than
        deadlock the producer against the writer.

        Returns ``(limits, reserve_bytes, effective_staging_bytes, source)``
        where ``source`` is ``"configured"`` or ``"fallback_default"``.
        """
        from .ready_queue import ReadyQueueLimits
        limits = self.ready_queue_limits
        effective = int(self.staging_max_gb * 1024**3)
        reserve = self.ready_queue_staging_reserve_bytes
        if limits.max_bytes + reserve <= effective:
            return limits, reserve, effective, "configured"

        d = self._READY_QUEUE_DEFAULTS
        defaults = ReadyQueueLimits(
            d['min_ready_bytes_before_writer_start'], d['target_ready_bytes'],
            d['max_ready_bytes'], d['max_ready_chunks'])
        if defaults.max_bytes + reserve <= effective:
            print(f"[CONFIG] [PIPELINE] max_ready_bytes "
                  f"({limits.max_bytes / 1024**3:.0f} GiB) + staging reserve "
                  f"({reserve / 1024**3:.0f} GiB) exceeds the effective staging "
                  f"budget ({effective / 1024**3:.0f} GiB from staging_max_gb="
                  f"{self.staging_max_gb:.0f}). Falling back atomically to the "
                  f"documented default ready-queue limits.")
            return defaults, reserve, effective, "fallback_default"

        raise ValueError(
            f"[PIPELINE] ready-queue limits cannot coexist with staging: even "
            f"the default max_ready_bytes ({defaults.max_bytes / 1024**3:.0f} "
            f"GiB) + reserve ({reserve / 1024**3:.0f} GiB) exceeds the effective "
            f"staging budget ({effective / 1024**3:.0f} GiB). Increase "
            f"staging_max_gb or reduce ready_queue_staging_reserve_bytes; "
            f"refusing to run with a queue that would starve fetch/pack.")

    @property
    def prefetch_chunks_ahead(self):
        return max(1, int(self._get_float(
            'PERFORMANCE', 'prefetch_chunks_ahead', 1)))
    @property
    def staging_max_gb(self):
        return self._get_float('PERFORMANCE', 'staging_max_gb', 350)
    @property
    def ram_soft_limit_pct(self):
        return self._get_float('PERFORMANCE', 'ram_soft_limit_pct', 70)
    @property
    def ram_hard_limit_pct(self):
        return self._get_float('PERFORMANCE', 'ram_hard_limit_pct', 85)
    @property
    def fetch_target_mbs(self):
        return self._get_float('PERFORMANCE', 'fetch_target_mbs', 100)
    @property
    def fetch_min_free_ram_gb(self):
        return self._get_float('PERFORMANCE', 'fetch_min_free_ram_gb', 16)
    @property
    def governor_fetch_target_free_ram_gb(self):
        return self._get_float(
            'PERFORMANCE', 'governor_fetch_target_free_ram_gb',
            min(self.fetch_min_free_ram_gb, 4.0))
    @property
    def governor_fetch_min_free_floor_gb(self):
        return self._get_float(
            'PERFORMANCE', 'governor_fetch_min_free_floor_gb', 2.5)
    @property
    def governor_fetch_total_ram_cap_pct(self):
        return self._get_float(
            'PERFORMANCE', 'governor_fetch_total_ram_cap_pct', 25)
    @property
    def governor_tape_min_free_ram_gb(self):
        return self._get_float(
            'PERFORMANCE', 'governor_tape_min_free_ram_gb', 3.0)
    @property
    def governor_tape_pause_other_stages(self):
        return self._get_bool(
            'PERFORMANCE', 'governor_tape_pause_other_stages', True)
    @property
    def governor_tape_exclusive_heavy_stages(self):
        return self._get_bool(
            'PERFORMANCE', 'governor_tape_exclusive_heavy_stages', True)
    @property
    def governor_soft_relax_after_seconds(self):
        return self._get_float(
            'PERFORMANCE', 'governor_soft_relax_after_seconds', 120)
    @property
    def governor_soft_relax_factor(self):
        return self._get_float(
            'PERFORMANCE', 'governor_soft_relax_factor', 0.75)
    @property
    def governor_status_interval_seconds(self):
        return self._get_float(
            'PERFORMANCE', 'governor_status_interval_seconds', 60)
    @property
    def governor_memory_sample_interval_seconds(self):
        return self._get_float(
            'PERFORMANCE', 'governor_memory_sample_interval_seconds', 5)
    @property
    def governor_metadata_batch_size(self):
        return self._get_int(
            'PERFORMANCE', 'governor_metadata_batch_size', 10000,
            minimum=1)
    @property
    def fetch_parallel_streams(self):
        """Number of concurrent SSH/tar fetch streams per chunk (default 1 =
        legacy single-stream). Small-file chunks are per-file-latency bound on a
        single stream; 2-3 parallel streams overlap those stalls up to the WAN
        ceiling. Bounded by cores/RAM — keep modest on small hosts."""
        return self._get_int(
            'PERFORMANCE', 'fetch_parallel_streams', 1, minimum=1)
    @property
    def fetch_transient_retries(self):
        """How many times to retry a fetch batch that failed on a *transient*
        network error (DNS resolution, ssh exit 255, connection reset/refused/
        timeout) before giving up the chunk. 0 restores the old fail-fast.

        This exists because on 2026-07-17 a single momentary DNS failure
        ("Could not resolve hostname srv01") killed the whole streaming session,
        and with the monitor offline the run then sat idle for ~3 days. A
        transient blip should cost a short backoff, not the run."""
        return self._get_int(
            'PERFORMANCE', 'fetch_transient_retries', 5, minimum=0)
    @property
    def fetch_transient_retry_base_seconds(self):
        """Base delay for the exponential backoff between transient-error
        retries (delay = base * 2**attempt, capped at 60s)."""
        return max(0.0, self._get_float(
            'PERFORMANCE', 'fetch_transient_retry_base_seconds', 5.0))
    @property
    def governor_pack_file_batch_size(self):
        return self._get_int(
            'PERFORMANCE', 'governor_pack_file_batch_size', 10000,
            minimum=1)
    @property
    def pack_parallel_workers(self):
        """Number of concurrent PACK worker threads (default 1 = legacy serial
        packer, byte-for-byte unchanged path). >1 shards each chunk's files
        across threads, each writing its own uniquely-named bundle(s)/
        manifest(s) through the identical per-file logic; all workers pass
        through the Resource Governor, so a tape write still pauses them all.
        Independent of fetch_parallel_streams and of the tape CPU affinity."""
        return self._get_int(
            'PERFORMANCE', 'pack_parallel_workers', 1, minimum=1)
    @property
    def tape_write_exclusive(self):
        return self._get_bool('PERFORMANCE', 'tape_write_exclusive', True)
    @property
    def allow_fetch_during_tape_write(self):
        return self._get_bool(
            'PERFORMANCE', 'allow_fetch_during_tape_write', False)
    @property
    def allow_pack_during_tape_write(self):
        return self._get_bool(
            'PERFORMANCE', 'allow_pack_during_tape_write', False)
    @property
    def allow_db_sync_during_tape_write(self):
        return self._get_bool(
            'PERFORMANCE', 'allow_db_sync_during_tape_write', False)
    @property
    def allow_db_sync_during_fetch(self):
        return self._get_bool(
            'PERFORMANCE', 'allow_db_sync_during_fetch', False)
    @property
    def allow_pack_during_fetch(self):
        return self._get_choice(
            'PERFORMANCE', 'allow_pack_during_fetch', 'conditional')
    @property
    def allow_pack_above_ram_soft(self):
        return self._get_bool(
            'PERFORMANCE', 'allow_pack_above_ram_soft', False)
    @property
    def allow_resume_oversized_chunks(self):
        return self._get_bool(
            'PERFORMANCE', 'allow_resume_oversized_chunks', False)
    @property
    def robocopy_priority(self):
        return self._get_choice('PERFORMANCE', 'robocopy_priority', 'high')
    @property
    def cpu_affinity(self):
        return self.config.get('PERFORMANCE', 'cpu_affinity', fallback='auto')
    @property
    def ssh_cipher(self):
        return self.config.get('PERFORMANCE', 'ssh_cipher', fallback='aes128-gcm@openssh.com').strip()
    @property
    def ssh_command_timeout_seconds(self):
        return max(1, int(self._get_float(
            'PERFORMANCE', 'ssh_command_timeout_seconds', 3600)))
    @property
    def use_mbuffer(self):
        return self._get_bool('PERFORMANCE', 'use_mbuffer', False)
    @property
    def mbuffer_size(self):
        return self.config.get('PERFORMANCE', 'mbuffer_size', fallback='512M').strip()
    @property
    def staging_padding_factor(self):
        return max(1.0, self._get_float(
            'PERFORMANCE', 'staging_padding_factor', 1.15))
    @property
    def fetch_overrun_abort_factor(self):
        """Abort a fetch when it exceeds its planned bytes by this factor
        (0 disables the hard abort; the overrun warning always fires)."""
        value = self._get_float(
            'PERFORMANCE', 'fetch_overrun_abort_factor', 2.0)
        return 0.0 if value <= 0 else max(1.0, value)

    @property
    def fetch_stall_timeout_seconds(self):
        """Abort a fetch whose staging dir has not grown by a single byte for
        this many seconds (0 disables). Guards against a wedged SSH/tar stream
        that stays connected but delivers no data — the abort is caught by the
        fetch retry/backoff, so the chunk stays resumable. The default is
        generous so a legitimately slow remote scan (tar stat-ing hundreds of
        thousands of files before the first byte) is never aborted."""
        return self._get_int('PERFORMANCE', 'fetch_stall_timeout_seconds',
                             600, minimum=0)

    @property
    def telegram_enabled(self):
        return self._get_bool('TELEGRAM', 'enabled', False)

    @property
    def telegram_timeout_seconds(self):
        return max(1, int(self._get_float('TELEGRAM', 'timeout_seconds', 10)))

    @property
    def telegram_heartbeat_minutes(self):
        """Interval for periodic all-is-well pipeline notifications
        (0 disables the heartbeat; alerts still fire)."""
        return max(0.0, self._get_float('TELEGRAM', 'heartbeat_minutes', 30.0))

    @property
    def stored_tar_sidecar_fixed_reserve_bytes(self):
        return self._get_int(
            'PERFORMANCE', 'stored_tar_sidecar_fixed_reserve_bytes',
            4096, minimum=0)

    @property
    def stored_tar_sidecar_member_reserve_bytes(self):
        return self._get_int(
            'PERFORMANCE', 'stored_tar_sidecar_member_reserve_bytes',
            1024, minimum=0)

    @property
    def telegram_bot_token(self):
        return _strip_quotes(
            os.environ.get('TELEGRAM_BOT_TOKEN')
            or self.env.get('TELEGRAM_BOT_TOKEN')
            or self.config.get('TELEGRAM', 'bot_token', fallback='', raw=True))

    @property
    def telegram_chat_id(self):
        return _strip_quotes(
            os.environ.get('TELEGRAM_CHAT_ID')
            or self.env.get('TELEGRAM_CHAT_ID')
            or self.config.get('TELEGRAM', 'chat_id', fallback='', raw=True))

    @property
    def local_manifest_archive_root(self):
        return _clean_config_path(self.config.get(
            'LOCAL_MANIFEST_ARCHIVE', 'root',
            fallback=os.path.join(PROJECT_ROOT, 'LOCAL_MANIFEST_ARCHIVE')))

    @property
    def local_manifest_search_limit(self):
        return self._get_int(
            'LOCAL_MANIFEST_ARCHIVE', 'search_limit', 500, minimum=1)

    @property
    def windows_update_guard(self):
        return self._get_bool('WINDOWS_UPDATE', 'guard', True)

    @property
    def windows_update_pause_days(self):
        return self._get_int('WINDOWS_UPDATE', 'pause_days', 7, minimum=1)

    @property
    def windows_update_block_on_pending_reboot(self):
        return self._get_bool(
            'WINDOWS_UPDATE', 'block_on_pending_reboot', True)

    @property
    def stored_tar_write_enabled(self):
        """Allow new Stored TAR assignment/production; fail-closed by default.

        Reader/restore support is intentionally independent so turning this off
        after the first TAR exists cannot strand an immutable archive object.
        """
        return self._get_bool(
            'FEATURES', 'stored_tar_write_enabled', False)

    @property
    def sealed_tape_write_batches_enabled(self):
        """Phase 5 sealed tape-write batches. Fail-closed default: False. While
        False the production runtime touches no batch table. Enabling it without
        the applied 012 schema must fail at startup (a later phase wires the
        preflight; Phase 5B keeps the repository unused by production)."""
        return self._get_bool(
            'FEATURES', 'sealed_tape_write_batches_enabled', False)

    @property
    def sealed_tape_write_batches_observation_enabled(self):
        """Phase 5C observation mode. Fail-closed default: False. When False no
        observer is constructed. When True the observer only computes/logs a
        shadow comparison and can never change production behaviour; an observer
        failure never blocks the writer. Enabling it does not enable sealed
        batches. Do not enable during Phase 5C."""
        return self._get_bool(
            'FEATURES', 'sealed_tape_write_batches_observation_enabled', False)

    # --- [OBSERVATION] : Phase 5D bounded async observation -------------------
    @property
    def sealed_batch_observation_queue_max(self):
        return self._get_int('OBSERVATION',
                             'sealed_batch_observation_queue_max', 100,
                             minimum=1)

    @property
    def sealed_batch_observation_shutdown_timeout_seconds(self):
        return self._get_float(
            'OBSERVATION',
            'sealed_batch_observation_shutdown_timeout_seconds', 5.0)

    @property
    def sealed_batch_observation_statement_timeout_seconds(self):
        return self._get_float(
            'OBSERVATION',
            'sealed_batch_observation_statement_timeout_seconds', 5.0)

    @property
    def sealed_batch_observation_log_path(self):
        return _clean_config_path(self.config.get(
            'OBSERVATION', 'sealed_batch_observation_log_path',
            fallback=os.path.join(tempfile.gettempdir(), 'lto8_observations',
                                  'sealed_batch_observations.jsonl')))

    @property
    def sealed_batch_observation_log_max_bytes(self):
        return self._get_int('OBSERVATION',
                             'sealed_batch_observation_log_max_bytes',
                             10 * 1024 * 1024, minimum=4096)

    @property
    def sealed_batch_observation_log_backups(self):
        return self._get_int('OBSERVATION',
                             'sealed_batch_observation_log_backups', 5,
                             minimum=0)
