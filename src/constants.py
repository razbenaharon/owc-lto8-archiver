"""Project-wide constants and tiny pure helpers shared across the package.

Path anchoring: the package modules live under ``src/`` but the application's
data files (``config.ini``, ``.env``, ``backup_logs/``) live
in the PROJECT ROOT — the parent of ``src/``. ``PROJECT_ROOT`` is therefore the
parent of this package directory, and the root runner scripts ``chdir()`` there
so a relative ``CONFIG_FILE`` resolves against the root exactly as before.
"""
import math
import os
from typing import NamedTuple

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(PACKAGE_DIR)

CONFIG_FILE = "config.ini"      # relative; runners chdir(PROJECT_ROOT)
LTFS_DIR = r'C:\Program Files\IBM\LTFS'  # IBM LTFS tools must run from this directory
BACKUP_LOG_DIR = os.path.join(PROJECT_ROOT, 'backup_logs')
DB_BACKUP_DIR = os.path.join(PROJECT_ROOT, 'db_backups')
# Surfaced to the operator at the start of every tape-write run. Internal tape
# I/O is serialized (see _acquire_tape_io_lock), but external processes are not.
LTFS_WRITE_WARNING = (
    "During archive writes, avoid browsing the LTFS drive or starting separate "
    "copy jobs. Internal tape access is serialized, but external processes can "
    "still degrade tape throughput."
)
# Per-tape planning budget. LTFS reports a free-space figure
# (shutil.disk_usage(...).free) that depends on hardware compression and is
# therefore only ADVISORY — this budget, not the live probe, is the real guard
# against overfilling a tape.
#
# The budget depends on the cartridge generation, chosen by
# ``config.ini [HARDWARE] tape_generation`` and resolved by
# ``ConfigManager.tape_profile``. There is deliberately NO module-level budget
# constant: every caller receives a ``TapeProfile`` explicitly, so a code path
# that forgot to ask the configuration fails loudly instead of silently
# planning an LTO-7 cartridge with the LTO-8 figure.
#
# The headroom is a code constant, not a setting: the margin can never be
# configured away.
TAPE_PLAN_HEADROOM = 0.95                        # leave 5% for LTFS overhead


class TapeProfile(NamedTuple):
    """Planning figures for one LTO cartridge generation.

    ``native_bytes`` is the native (uncompressed) capacity printed on the
    cartridge, in decimal bytes. ``nominal_bytes`` is what the headroom is
    applied to. ``register_gb`` is the decimal-GB figure a fresh cartridge is
    registered with in ``tapes.total_capacity``. ``configured`` is True when
    the generation was chosen in config.ini, False for the legacy default.
    """
    generation: str
    native_bytes: int
    nominal_bytes: int
    register_gb: int
    configured: bool = False

    @property
    def budget_bytes(self):
        return int(self.nominal_bytes * TAPE_PLAN_HEADROOM)

    @property
    def label(self):
        """Human label for the budget, e.g. "11.5 TB"."""
        return f"{self.budget_bytes / 1000**4:.1f} TB"

    @property
    def native_tb_text(self):
        """Native capacity in decimal TB, e.g. "12" or "2.5"."""
        return f"{self.native_bytes / 1000**4:g}"


# Keyed by the lower-case spelling config.py accepts. LTO-8 keeps the legacy
# figures verbatim (11 TiB nominal, 12288 GB registration) so choosing it
# explicitly plans exactly what the archiver always planned. Every other
# generation applies the headroom to its native decimal capacity. LTO-10 is
# absent on purpose: it ships as 30 TB and 40 TB cartridges, so it needs its
# own reviewed entry rather than a guess.
TAPE_PROFILES = {
    'lto-6': TapeProfile('LTO-6', 2_500 * 1000**3, 2_500 * 1000**3, 2500),
    'lto-7': TapeProfile('LTO-7', 6 * 1000**4, 6 * 1000**4, 6000),
    'lto-m8': TapeProfile('LTO-M8', 9 * 1000**4, 9 * 1000**4, 9000),
    'lto-8': TapeProfile('LTO-8', 12 * 1000**4, 11 * 1024**4, 12288),
    'lto-9': TapeProfile('LTO-9', 18 * 1000**4, 18 * 1000**4, 18000),
}
# What a config.ini without ``tape_generation`` gets: the LTO-8 figures the
# archiver has always used (budget 11,489,896,510,259 bytes, "11.5 TB",
# registration default 12288 GB), with the legacy capacity rule.
LEGACY_TAPE_PROFILE = TAPE_PROFILES['lto-8']


def registered_budget(total_capacity_gb):
    """Planning bytes for a cartridge from its registered capacity.

    A registration equal to some generation's ``register_gb`` gets that
    generation's budget — so a cartridge registered with the legacy LTO-8
    figure (12288 GB, which is larger than LTO-8's native 12000 GB) keeps the
    legacy LTO-8 budget on a host configured for a larger generation. Any
    other figure gets the headroom applied to it.
    """
    gb = float(total_capacity_gb)
    for profile in TAPE_PROFILES.values():
        if gb == profile.register_gb:
            return profile.budget_bytes
    return int(gb * 1000**3 * TAPE_PLAN_HEADROOM)


def registered_capacity_problem(capacity_gb, profile):
    """Why ``capacity_gb`` may not be registered, or None when it may.

    Only a configured generation is checked (the legacy default keeps the
    legacy behaviour). A figure above the configured generation's native
    capacity cannot be a cartridge this drive writes: it is almost always the
    compressed figure printed next to the native one on the box.
    """
    if not profile.configured:
        return None
    if not math.isfinite(capacity_gb) or capacity_gb <= 0:
        return "capacity must be a finite number greater than 0 GB."
    if capacity_gb > profile.register_gb:
        return (f"{capacity_gb:g} GB is more than an {profile.generation} "
                f"cartridge holds ({profile.register_gb} GB native). Enter the "
                "NATIVE capacity, not the compressed figure.")
    return None


def tape_profile_banner(profile):
    """One line naming the active budget, printed before every tape write."""
    source = ("from config.ini [HARDWARE] tape_generation" if profile.configured
              else "default; [HARDWARE] tape_generation is not set")
    return (f"Tape budget: {profile.generation}, {profile.label} per cartridge "
            f"({source}).")


# tapes.status values (migration 011). 'full' = retired from writing, whatever
# the reason — genuinely out of space, or unwritable like Tape_02 after its PWE
# bit latched (docs/history/incidents/010-20260724-ltfs-write-perm-readonly.md).
TAPE_STATUS_ACTIVE = 'active'
TAPE_STATUS_FULL = 'full'
TAPE_STATUSES = (TAPE_STATUS_ACTIVE, TAPE_STATUS_FULL)


def tape_is_full(status):
    """True when a ``tapes.status`` value retires the tape from writing.

    ``None`` (column absent on a pre-011 database, or an unregistered tape)
    reads as active, so the guard is inert until a tape is marked.
    """
    return str(status or TAPE_STATUS_ACTIVE).strip().lower() == TAPE_STATUS_FULL


def tape_status_reason_suffix(tape):
    """Return ``" (reason)"`` for a tape row, or ``""`` when none is recorded."""
    reason = ((tape or {}).get('status_reason') or '').strip()
    return f" ({reason})" if reason else ''


def tape_budget_bytes(total_capacity_gb, used_bytes, reserved_bytes=0,
                      status=None, *, profile):
    """Return ``(capacity_bytes, available_bytes)`` for a registered tape.

    ``total_capacity_gb`` is the ``tapes.total_capacity`` column — a
    decimal-GB figure (see ``TapeProfile.register_gb``). The planning
    capacity is the smaller of the profile's budget and the registered
    capacity. Decimal GB replaces a previous GiB interpretation that
    overstated capacity by ~7%; the change can only ever reject MORE, never
    accept more than before.

    With the legacy default profile the registered capacity is used raw,
    exactly as before. With a configured generation it goes through
    ``registered_budget`` so it carries the headroom too: the profile budget
    is then no longer the only margin protecting an older, smaller
    cartridge.

    ``status`` is ``tapes.status``: a tape marked ``full`` reports zero
    available bytes regardless of its counters, which is the only
    representation that survives ``recalculate_tape_used_space`` rewriting
    ``used_space`` from the catalog on every mount.
    """
    capacity_bytes = profile.budget_bytes
    if total_capacity_gb:
        if profile.configured:
            registered = registered_budget(total_capacity_gb)
        else:
            registered = int(float(total_capacity_gb) * 1000**3)
        capacity_bytes = min(capacity_bytes, registered)
    if tape_is_full(status):
        return capacity_bytes, 0
    available_bytes = max(
        0, capacity_bytes - int(used_bytes) - int(reserved_bytes))
    return capacity_bytes, available_bytes


class TapeUsage(NamedTuple):
    """How full a tape is, measured the way the planner measures it."""
    used_bytes: int
    budget_bytes: int
    available_bytes: int
    percent: float

    @property
    def percent_text(self):
        return f"{self.percent:.1f}%"

    @property
    def bar_fraction(self):
        """``percent`` as a 0..1 fraction, clamped for drawing a bar."""
        return min(max(self.percent / 100.0, 0.0), 1.0)


def tape_usage(total_capacity_gb, used_bytes, status=None, *, profile):
    """The single source of truth for a tape's displayed usage percentage.

    Used bytes over the planning budget from ``tape_budget_bytes`` — the
    figure the archiver actually refuses on. The GUI used to divide by the
    registered capacity read as GiB and the text menu by the same number read
    as GB, so one cartridge showed 80.5% in one place and 86.5% in the other
    while planning already had it at 92.5%. Every display must call this.
    """
    used = int(used_bytes or 0)
    budget_bytes, available_bytes = tape_budget_bytes(
        total_capacity_gb, used, status=status, profile=profile)
    percent = (used / budget_bytes * 100.0) if budget_bytes else 0.0
    return TapeUsage(used, budget_bytes, available_bytes, percent)


# Seal a bundle ZIP slightly before max_zip_size_gb so the ZIP central
# directory and per-entry headers never push it past the configured cap.
ZIP_BUNDLE_FILL_FACTOR = 0.99
ROOT_FILES_GROUP = "_ROOT_FILES"
# Fallback source-host label for catalog rows that predate per-host tracking or
# were written without an explicit source. Kept as the historical primary remote
# host so existing rows keep their label; centralized here so the value is not
# scattered as a bare string literal across the DB layer.
LEGACY_DEFAULT_SOURCE_HOST = "srv02"
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
