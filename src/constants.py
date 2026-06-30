"""Project-wide constants and tiny pure helpers shared across the package.

Path anchoring: the package modules live under ``src/`` but the application's
data files (``config.ini``, ``.env``, ``lto_archive.db``, ``backup_logs/``) live
in the PROJECT ROOT — the parent of ``src/``. ``PROJECT_ROOT`` is therefore the
parent of this package directory, and the root runner scripts ``chdir()`` there
so a relative ``CONFIG_FILE`` resolves against the root exactly as before.
"""
import os

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(PACKAGE_DIR)

BUFFER_SIZE = 1024 * 1024 * 16  # 16 MB read buffer
CONFIG_FILE = "config.ini"      # relative; runners chdir(PROJECT_ROOT)
LTFS_DIR = r'C:\Program Files\IBM\LTFS'  # IBM LTFS tools must run from this directory
BACKUP_LOG_DIR = os.path.join(PROJECT_ROOT, 'backup_logs')
# Surfaced to the operator at the start of every tape-write run. Internal tape
# I/O is serialized (see _acquire_tape_io_lock), but external processes are not.
LTFS_WRITE_WARNING = (
    "During archive writes, avoid browsing the LTFS drive or starting separate "
    "copy jobs. Internal tape access is serialized, but external processes can "
    "still degrade tape throughput."
)
# Per-tape planning budget for the local bin-packer. LTFS reports a free-space
# figure (shutil.disk_usage(...).free) that depends on hardware compression and
# is therefore only ADVISORY — this budget, not the live probe, is the real
# guard against overfilling a tape. Expressed on a single binary basis (TiB)
# with an explicit headroom margin. The result is kept at/below the previous
# 11.5 TB budget so the planner only ever rejects MORE, never accepts more than
# before.
TAPE_PLAN_NOMINAL_BYTES = 11 * 1024**4          # 11 TiB nominal (binary basis)
TAPE_PLAN_HEADROOM = 0.95                        # leave 5% for LTFS overhead
LOCAL_TAPE_BUDGET_BYTES = int(TAPE_PLAN_NOMINAL_BYTES * TAPE_PLAN_HEADROOM)
ROOT_FILES_GROUP = "_ROOT_FILES"
AUTO_PACK_FILE_RATIO = 0.30
AUTO_PACK_MIN_SMALL_BYTES = 1 * 1024**3
AUTO_PACK_MIN_SMALL_BYTE_RATIO = 0.01
DB_UPSERT_BATCH_SIZE = 10_000
LOCAL_STAGING_RESERVE_BYTES = 20 * 1024**3


def _auto_pack_decision(total_files, total_bytes, small_files, small_bytes):
    file_ratio = (small_files / total_files) if total_files else 0.0
    byte_ratio = (small_bytes / total_bytes) if total_bytes else 0.0
    meaningful_size = (
        small_bytes >= AUTO_PACK_MIN_SMALL_BYTES or
        byte_ratio >= AUTO_PACK_MIN_SMALL_BYTE_RATIO
    )
    should_pack = file_ratio > AUTO_PACK_FILE_RATIO and meaningful_size
    return should_pack, file_ratio, byte_ratio
