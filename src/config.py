"""ConfigManager and .env loading."""
import os
import configparser

try:
    import psutil
except ImportError:  # optional dependency — priority/affinity degrade gracefully
    psutil = None

from .constants import BACKUP_LOG_DIR, CONFIG_FILE, PROJECT_ROOT
from .paths import _clean_config_path, _clean_remote_path, _config_list


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
            'db_path':     os.path.join(PROJECT_ROOT, 'lto_archive.db'),
            'backup_log_dir': BACKUP_LOG_DIR,
        }
        self.config['HARDWARE'] = {
            'lto_drive':     r'D:\\',
            'ibm_eject_cmd': r'C:\Program Files\IBM\LTFS\LtfsCmdEject.exe',
        }
        self.config['SETTINGS'] = {
            'zip_threshold_mb': '100',
            'max_zip_size_gb':  '100',
        }
        self.config['REMOTE'] = {
            'remote_host':      'your.remote.host',
            'remote_user':      '',
            'remote_password':  '',
            'remote_path':      '',
            'remote_selected_paths': '',
            'confirm_before_backup': 'true',
            'staging_fill_pct': '0.80',
        }
        self.config['PERFORMANCE'] = {
            'chunk_cap_gb':          '100',
            'prefetch_chunks_ahead': '2',
            'eject_after_pack':      'off',
            'staging_max_gb':        '350',
            'robocopy_priority':     'high',
            'cpu_affinity':          'auto',
            'ssh_cipher':            'aes128-gcm@openssh.com',
            'use_mbuffer':           'true',
            'mbuffer_size':          '2G',
        }
        with open(self.config_path, 'w', encoding='utf-8') as f:
            self.config.write(f)

    @property
    def source_dir(self):    return _clean_config_path(self.config['PATHS']['source_dir'])
    @property
    def staging_dir(self):   return _clean_config_path(self.config['PATHS']['staging_dir'])
    @property
    def restore_dir(self):   return _clean_config_path(self.config['PATHS']['restore_dir'])
    @property
    def db_path(self):       return _clean_config_path(self.config['PATHS']['db_path'])
    @property
    def backup_log_dir(self):
        return _clean_config_path(self.config.get('PATHS', 'backup_log_dir',
                                                  fallback=BACKUP_LOG_DIR))
    @property
    def lto_drive(self):     return _clean_config_path(self.config['HARDWARE']['lto_drive'])
    @property
    def ibm_eject_cmd(self): return _clean_config_path(self.config['HARDWARE'].get(
                                 'ibm_eject_cmd',
                                 r'C:\Program Files\IBM\LTFS\LtfsCmdEject.exe'))
    @property
    def zip_threshold_mb(self): return float(self.config['SETTINGS']['zip_threshold_mb'])
    @property
    def max_zip_size_gb(self):  return float(self.config['SETTINGS']['max_zip_size_gb'])
    @property
    def remote_host(self):      return self.config.get('REMOTE', 'remote_host', fallback='')
    @property
    def remote_user(self):      return self.config.get('REMOTE', 'remote_user', fallback='')
    @property
    def remote_password(self):
        # Priority: process env var > .env file > config.ini (kept empty in git).
        value = (os.environ.get('REMOTE_PASSWORD')
                 or self.env.get('REMOTE_PASSWORD')
                 or self.config.get('REMOTE', 'remote_password', fallback='', raw=True))
        value = (value or '').strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        return value
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
    def staging_fill_pct(self): return float(self.config.get('REMOTE', 'staging_fill_pct', fallback='0.80'))

    # --- [PERFORMANCE] : continuous-streaming pipeline tuning -----------------
    @property
    def chunk_cap_gb(self):
        return float(self.config.get('PERFORMANCE', 'chunk_cap_gb', fallback='100'))
    @property
    def prefetch_chunks_ahead(self):
        return max(1, int(float(self.config.get('PERFORMANCE', 'prefetch_chunks_ahead', fallback='2'))))
    @property
    def eject_after_pack(self):
        raw = self.config.get('PERFORMANCE', 'eject_after_pack', fallback='off').strip().lower()
        if raw in ('', 'off', 'none', 'false', 'no'):
            return None
        try:
            value = int(float(raw))
        except ValueError:
            return None
        return value if value >= 0 else None
    @property
    def staging_max_gb(self):
        return float(self.config.get('PERFORMANCE', 'staging_max_gb', fallback='350'))
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
    def use_mbuffer(self):
        return self.config.get('PERFORMANCE', 'use_mbuffer', fallback='true').strip().lower() in ('1', 'true', 'yes', 'on')
    @property
    def mbuffer_size(self):
        return self.config.get('PERFORMANCE', 'mbuffer_size', fallback='2G').strip()
