"""Rotating file log for diagnostic traces.

The console remains the operator UI (prints, progress lines, phase banners —
all unchanged); this module only adds a durable trace file so a failure deep
into a multi-day run leaves evidence beyond terminal scrollback. Status lines
from :mod:`src.runtime` and exception tracebacks from the CLI entry points tee
into ``<backup_log_dir>/archiver.log``.

Never log secrets: messages must not include ``remote_password``, DSNs with
passwords, or Telegram tokens. The existing status/error messages already obey
this — keep it that way when adding log calls.
"""
import logging
import logging.handlers
import os

from .constants import BACKUP_LOG_DIR

LOG_FILE_NAME = 'archiver.log'
_LOGGER_NAME = 'lto'
_CONFIGURED = False


def get_logger():
    """Return the shared app logger.

    Before :func:`configure_file_logging` runs (or if it failed) the logger
    has no handlers and ``propagate`` disabled, so calls are silent no-ops —
    modules can log unconditionally.
    """
    logger = logging.getLogger(_LOGGER_NAME)
    logger.propagate = False
    return logger


def configure_file_logging(log_dir=None):
    """Attach the rotating file handler once per process (idempotent).

    A handler-creation failure (unwritable directory, locked file) degrades to
    a printed warning — diagnostics must never break an archive run.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return
    logger = get_logger()
    log_dir = os.path.abspath(log_dir or BACKUP_LOG_DIR)
    try:
        os.makedirs(log_dir, exist_ok=True)
        handler = logging.handlers.RotatingFileHandler(
            os.path.join(log_dir, LOG_FILE_NAME),
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding='utf-8',
        )
    except OSError as e:
        print(f"[LOG] Warning: could not open {LOG_FILE_NAME} in "
              f"{log_dir}: {e}. Continuing without a file log.")
        _CONFIGURED = True  # do not retry (and re-warn) on every call
        return
    handler.setFormatter(
        logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    _CONFIGURED = True
