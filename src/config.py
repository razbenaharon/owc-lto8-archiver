"""ConfigManager and .env loading."""
import os
import configparser
from urllib.parse import quote

from .constants import BACKUP_LOG_DIR, CONFIG_FILE, PROJECT_ROOT
from .paths import _clean_config_path, _clean_remote_path, _config_list


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
    def __init__(self, config_path=CONFIG_FILE):
        self.config      = configparser.ConfigParser(interpolation=None)
        self.config_path = config_path

        if not os.path.exists(config_path):
            self._create_default()
            print(f"[CONFIG] Created default config file: {os.path.abspath(config_path)}")
            print("[CONFIG] Please review and edit it before running operations.")

        self.config.read(config_path, encoding='utf-8')

        # Secrets live in a gitignored .env next to the app, never in config.ini.
        self.env = _load_env_file(os.path.join(PROJECT_ROOT, '.env'))

    def _create_default(self):
        self.config['PATHS'] = {
            'source_dir':  os.path.join(PROJECT_ROOT, 'source'),
            'staging_dir': os.path.join(PROJECT_ROOT, 'staging'),
            'restore_dir': os.path.join(PROJECT_ROOT, 'restored'),
            'backup_log_dir': BACKUP_LOG_DIR,
        }
        self.config['DATABASE'] = {
            'host': 'localhost',
            'port': '5432',
            'dbname': 'lto_archive',
            'user': 'lto',
            'sslmode': 'prefer',
        }
        self.config['HARDWARE'] = {
            'lto_drive':     r'D:\\',
            'ibm_eject_cmd': r'C:\Program Files\IBM\LTFS\LtfsCmdEject.exe',
        }
        self.config['SETTINGS'] = {
            'zip_threshold_mb': '100',
            'max_zip_size_gb':  '100',
        }
        self.config['CATALOG'] = {
            'index_min_file_mb': '10',
            'index_packed_small_files': 'false',
            'index_directory_stats': 'true',
            'index_full_directory_tree': 'true',
            'prefer_directory_containers': 'true',
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
            'staging_fill_pct': '0.80',
            'scan_mode': 'directories',
            'remote_scan_depth': '2',
            'large_file_min_mb': '10',
            'directory_chunk_max_gb': '50',
            'directory_chunk_max_files': '100000',
        }
        self.config['PERFORMANCE'] = {
            'pipeline_profile':      'tape_first_controlled',
            'chunk_cap_gb':          '50',
            'chunk_max_files':       '100000',
            'prefetch_chunks_ahead': '1',
            'staging_max_gb':        '350',
            'ram_soft_limit_pct':    '70',
            'ram_hard_limit_pct':    '85',
            'fetch_target_mbs':      '100',
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
        }
        self.config['TELEGRAM'] = {
            'enabled': 'false',
            'timeout_seconds': '10',
            'heartbeat_minutes': '30',
        }
        self.config['COLD_MANIFEST_DB'] = {
            'enabled': 'true',
            'pause_during_archive': 'true',
            'host': 'localhost',
            'port': '55432',
            'database': 'lto_cold_manifest',
            'user': 'lto_cold',
            'password_env': 'COLD_PGPASSWORD',
            'sslmode': 'prefer',
            'small_file_threshold_mb': '10',
            'batch_rows': '25000',
            'fetch_rows': '5000',
            'sleep_between_batches_ms': '250',
            'max_ram_pct': '60',
            'min_free_ram_gb': '16',
            'max_local_disk_io_mbs': '200',
        }
        with open(self.config_path, 'w', encoding='utf-8') as f:
            self.config.write(f)

    def _get_float(self, section, key, fallback):
        """Read a float option; a typo degrades to the default with a warning
        at read time instead of an unhandled ValueError mid-run."""
        raw = self.config.get(section, key, fallback=str(fallback))
        try:
            return float(str(raw).strip())
        except ValueError:
            print(f"[CONFIG] Invalid [{section}] {key} = {raw!r}; "
                  f"using default {fallback}.")
            return float(fallback)

    def _get_bool(self, section, key, fallback):
        raw = self.config.get(section, key, fallback=str(fallback))
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in ('1', 'true', 'yes', 'on')

    def _get_int(self, section, key, fallback, minimum=None):
        raw = self.config.get(section, key, fallback=str(fallback)).strip()
        try:
            value = int(float(raw))
        except ValueError:
            print(f"[CONFIG] Invalid [{section}] {key} = {raw!r}; "
                  f"using default {fallback}.")
            value = int(fallback)
        if minimum is not None:
            value = max(minimum, value)
        return value

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
    def db_dsn(self):
        user = quote(self.pg_user, safe='')
        password = quote(self.pg_password, safe='')
        auth = f"{user}:{password}@" if password else f"{user}@"
        return (
            f"postgresql://{auth}{self.pg_host}:{self.pg_port}/"
            f"{quote(self.pg_dbname, safe='')}?sslmode={quote(self.pg_sslmode, safe='')}"
        )
    @property
    def db_display_ref(self):
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
    def zip_threshold_mb(self):
        return self._get_float('SETTINGS', 'zip_threshold_mb', 100)
    @property
    def max_zip_size_gb(self):
        return self._get_float('SETTINGS', 'max_zip_size_gb', 100)
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
        return self.config.get(
            'CATALOG', 'small_file_manifest_format',
            fallback='jsonl').strip().lower()
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
        return self.config.get('REMOTE', 'confirm_before_backup', fallback='true').strip().lower() in ('1', 'true', 'yes', 'on')
    @property
    def staging_fill_pct(self):
        return self._get_float('REMOTE', 'staging_fill_pct', 0.80)
    @property
    def remote_scan_mode(self):
        return self.config.get(
            'REMOTE', 'scan_mode', fallback='directories').strip().lower()
    @property
    def remote_scan_depth(self):
        return self._get_int('REMOTE', 'remote_scan_depth', 2, minimum=0)
    @property
    def large_file_min_mb(self):
        return self._get_float('REMOTE', 'large_file_min_mb',
                               self.index_min_file_mb)
    @property
    def directory_chunk_max_gb(self):
        return self._get_float('REMOTE', 'directory_chunk_max_gb',
                               self.chunk_cap_gb)
    @property
    def directory_chunk_max_files(self):
        return self._get_int('REMOTE', 'directory_chunk_max_files',
                             self.chunk_max_files, minimum=1)

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
    def governor_pack_file_batch_size(self):
        return self._get_int(
            'PERFORMANCE', 'governor_pack_file_batch_size', 10000,
            minimum=1)
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
        return self.config.get(
            'PERFORMANCE', 'allow_pack_during_fetch',
            fallback='conditional').strip().lower()
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
        return self.config.get('PERFORMANCE', 'robocopy_priority', fallback='high').strip().lower()
    @property
    def cpu_affinity(self):
        return self.config.get('PERFORMANCE', 'cpu_affinity', fallback='auto')
    @property
    def ssh_cipher(self):
        return self.config.get('PERFORMANCE', 'ssh_cipher', fallback='aes128-gcm@openssh.com').strip()
    @property
    def ssh_command_timeout_seconds(self):
        raw = self.config.get('PERFORMANCE', 'ssh_command_timeout_seconds',
                              fallback='3600').strip()
        try:
            value = int(float(raw))
        except ValueError:
            return 3600
        return max(1, value)
    @property
    def use_mbuffer(self):
        return self.config.get('PERFORMANCE', 'use_mbuffer', fallback='false').strip().lower() in ('1', 'true', 'yes', 'on')
    @property
    def mbuffer_size(self):
        return self.config.get('PERFORMANCE', 'mbuffer_size', fallback='512M').strip()
    @property
    def staging_padding_factor(self):
        raw = self.config.get('PERFORMANCE', 'staging_padding_factor',
                              fallback='1.15').strip()
        try:
            value = float(raw)
        except ValueError:
            return 1.15
        return max(1.0, value)
    @property
    def fetch_overrun_abort_factor(self):
        """Abort a fetch when it exceeds its planned bytes by this factor
        (0 disables the hard abort; the overrun warning always fires)."""
        raw = self.config.get('PERFORMANCE', 'fetch_overrun_abort_factor',
                              fallback='2.0').strip()
        try:
            value = float(raw)
        except ValueError:
            return 2.0
        return 0.0 if value <= 0 else max(1.0, value)

    @property
    def telegram_enabled(self):
        return self.config.get('TELEGRAM', 'enabled', fallback='false').strip().lower() in (
            '1', 'true', 'yes', 'on')

    @property
    def telegram_timeout_seconds(self):
        raw = self.config.get('TELEGRAM', 'timeout_seconds',
                              fallback='10').strip()
        try:
            value = int(float(raw))
        except ValueError:
            return 10
        return max(1, value)

    @property
    def telegram_heartbeat_minutes(self):
        """Interval for periodic all-is-well pipeline notifications
        (0 disables the heartbeat; alerts still fire)."""
        raw = self.config.get('TELEGRAM', 'heartbeat_minutes',
                              fallback='30').strip()
        try:
            value = float(raw)
        except ValueError:
            return 30.0
        return max(0.0, value)

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
    def cold_manifest_enabled(self):
        return self._get_bool('COLD_MANIFEST_DB', 'enabled', True)

    @property
    def cold_pause_during_archive(self):
        """Stop the cold-manifest Docker DB during a tape archive run so it does
        not compete for RAM with fetch/pack/tape. Restored automatically when the
        run ends. Set false to leave the cold DB running during archives."""
        return self._get_bool(
            'COLD_MANIFEST_DB', 'pause_during_archive', True)

    @property
    def cold_pg_host(self):
        return self.config.get(
            'COLD_MANIFEST_DB', 'host', fallback='localhost').strip()

    @property
    def cold_pg_port(self):
        return self.config.get(
            'COLD_MANIFEST_DB', 'port', fallback='55432').strip()

    @property
    def cold_pg_dbname(self):
        return self.config.get(
            'COLD_MANIFEST_DB', 'database',
            fallback='lto_cold_manifest').strip()

    @property
    def cold_pg_user(self):
        return self.config.get(
            'COLD_MANIFEST_DB', 'user', fallback='lto_cold').strip()

    @property
    def cold_pg_password_env(self):
        return self.config.get(
            'COLD_MANIFEST_DB', 'password_env',
            fallback='COLD_PGPASSWORD').strip()

    @property
    def cold_pg_password(self):
        env_name = self.cold_pg_password_env or 'COLD_PGPASSWORD'
        return _strip_quotes(
            os.environ.get(env_name)
            or self.env.get(env_name)
            or self.config.get('COLD_MANIFEST_DB', 'password',
                               fallback='', raw=True))

    @property
    def cold_pg_sslmode(self):
        return self.config.get(
            'COLD_MANIFEST_DB', 'sslmode', fallback='prefer').strip()

    @property
    def cold_db_dsn(self):
        user = quote(self.cold_pg_user, safe='')
        password = quote(self.cold_pg_password, safe='')
        auth = f"{user}:{password}@" if password else f"{user}@"
        return (
            f"postgresql://{auth}{self.cold_pg_host}:{self.cold_pg_port}/"
            f"{quote(self.cold_pg_dbname, safe='')}?sslmode={quote(self.cold_pg_sslmode, safe='')}"
        )

    @property
    def cold_db_display_ref(self):
        user = quote(self.cold_pg_user, safe='')
        auth = f"{user}:***@" if self.cold_pg_password else f"{user}@"
        return (
            f"postgresql://{auth}{self.cold_pg_host}:{self.cold_pg_port}/"
            f"{quote(self.cold_pg_dbname, safe='')}?sslmode={quote(self.cold_pg_sslmode, safe='')}"
        )

    @property
    def cold_small_file_threshold_mb(self):
        return self._get_float(
            'COLD_MANIFEST_DB', 'small_file_threshold_mb', 10)

    @property
    def cold_batch_rows(self):
        return self._get_int('COLD_MANIFEST_DB', 'batch_rows', 25000,
                             minimum=1)

    @property
    def cold_fetch_rows(self):
        return self._get_int('COLD_MANIFEST_DB', 'fetch_rows', 5000,
                             minimum=1)

    @property
    def cold_sleep_between_batches_ms(self):
        return self._get_int(
            'COLD_MANIFEST_DB', 'sleep_between_batches_ms', 250,
            minimum=0)

    @property
    def cold_max_ram_pct(self):
        return self._get_float('COLD_MANIFEST_DB', 'max_ram_pct', 60)

    @property
    def cold_min_free_ram_gb(self):
        return self._get_float('COLD_MANIFEST_DB', 'min_free_ram_gb', 16)

    @property
    def cold_max_local_disk_io_mbs(self):
        return self._get_float(
            'COLD_MANIFEST_DB', 'max_local_disk_io_mbs', 200)
