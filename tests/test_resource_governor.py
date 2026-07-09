import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from src.backup import LTOBackup
from src.resource_governor import ResourceGovernor


def _cfg(**overrides):
    data = {
        "staging_dir": ".",
        "ram_soft_limit_pct": 70,
        "ram_hard_limit_pct": 85,
        "fetch_min_free_ram_gb": 16,
        "governor_fetch_target_free_ram_gb": 4.0,
        "governor_fetch_min_free_floor_gb": 2.5,
        "governor_fetch_total_ram_cap_pct": 25,
        "governor_tape_min_free_ram_gb": 3.0,
        "governor_tape_exclusive_heavy_stages": True,
        "governor_status_interval_seconds": 60,
        "governor_soft_relax_after_seconds": 120,
        "governor_soft_relax_factor": 0.75,
        "tape_write_exclusive": True,
        "allow_fetch_during_tape_write": False,
        "allow_pack_during_tape_write": False,
        "allow_db_sync_during_tape_write": False,
        "allow_db_sync_during_fetch": False,
        "allow_pack_during_fetch": "conditional",
        "allow_pack_above_ram_soft": False,
    }
    data.update(overrides)
    return SimpleNamespace(**data)


def _vm(percent=20, available=64 * 1024**3, total=64 * 1024**3):
    return SimpleNamespace(percent=percent, available=available, total=total)


def _disk(free=10**12):
    return SimpleNamespace(total=free, used=0, free=free)


class ResourceGovernorTests(unittest.TestCase):
    def _governor(self, cfg=None, vm=None, disk=None):
        cfg = cfg or _cfg()
        gov = ResourceGovernor(cfg, staging_dir=cfg.staging_dir,
                               sleep_seconds=0.01)
        patches = [
            mock.patch("src.resource_governor.psutil.virtual_memory",
                       return_value=vm or _vm()),
            mock.patch("src.resource_governor.shutil.disk_usage",
                       return_value=disk or _disk()),
        ]
        return gov, patches

    def test_blocks_fetch_during_tape_write(self):
        gov, patches = self._governor()
        with patches[0], patches[1], gov.mark_tape_write_active():
            self.assertFalse(gov.can_start_fetch())

    def test_blocks_pack_during_tape_write(self):
        gov, patches = self._governor()
        with patches[0], patches[1], gov.mark_tape_write_active():
            self.assertFalse(gov.can_start_pack())

    def test_blocks_db_sync_during_tape_write(self):
        gov, patches = self._governor()
        with patches[0], patches[1], gov.mark_tape_write_active():
            self.assertFalse(gov.can_start_db_sync())

    def test_blocks_heavy_work_above_hard_ram_threshold(self):
        gov, patches = self._governor(vm=_vm(percent=90))
        with patches[0], patches[1]:
            self.assertFalse(gov.can_start_fetch())
            self.assertFalse(gov.can_start_pack())
            self.assertFalse(gov.can_start_db_sync())
            self.assertFalse(gov.can_start_tape_write())

    def test_pack_blocked_above_soft_ram_threshold_by_default(self):
        gov, patches = self._governor(vm=_vm(percent=75))
        with patches[0], patches[1]:
            self.assertFalse(gov.can_start_pack())

    def test_pack_above_soft_ram_threshold_can_be_overridden(self):
        gov, patches = self._governor(
            cfg=_cfg(allow_pack_above_ram_soft=True),
            vm=_vm(percent=75))
        with patches[0], patches[1]:
            self.assertTrue(gov.can_start_pack())

    def test_fetch_target_clamped_to_total_ram_pct(self):
        gov, patches = self._governor(
            cfg=_cfg(governor_fetch_target_free_ram_gb=16,
                     governor_fetch_total_ram_cap_pct=25,
                     governor_fetch_min_free_floor_gb=2.5),
            vm=_vm(percent=40, available=3 * 1024**3, total=16 * 1024**3))
        with patches[0], patches[1]:
            decision = gov.decision("fetch", "start")
        self.assertEqual(decision.effective_min_free_gb, 4.0)
        self.assertIn("fetch_min_free_ram", decision.reasons)

    def test_fetch_relaxation_never_goes_below_floor(self):
        gov, patches = self._governor(
            cfg=_cfg(governor_fetch_target_free_ram_gb=16,
                     governor_fetch_total_ram_cap_pct=100,
                     governor_fetch_min_free_floor_gb=2.5,
                     governor_soft_relax_after_seconds=1,
                     governor_soft_relax_factor=0.5),
            vm=_vm(percent=40, available=3 * 1024**3, total=64 * 1024**3))
        with patches[0], patches[1]:
            decision = gov.decision("fetch", "start", wait_seconds=10)
        self.assertEqual(decision.effective_min_free_gb, 2.5)
        self.assertTrue(decision.allowed)

    def test_decision_reports_distinct_blockers(self):
        gov, patches = self._governor(
            vm=_vm(percent=90, available=1 * 1024**3))
        with patches[0], patches[1], gov.mark_tape_write_active():
            decision = gov.decision(
                "fetch", "start", needed_bytes=10**13)
        self.assertIn("hard_ram_limit", decision.reasons)
        self.assertIn("fetch_min_free_ram", decision.reasons)
        self.assertIn("tape_active", decision.reasons)
        self.assertIn("staging_reserve", decision.reasons)

    def test_cold_migration_blocks_during_tape_write(self):
        gov, patches = self._governor()
        with patches[0], patches[1], \
             mock.patch.object(gov, "_local_disk_io_busy", return_value=False), \
             gov.mark_tape_write_active():
            self.assertFalse(gov.can_start_cold_migration())

    def test_cold_migration_blocks_during_fetch(self):
        gov, patches = self._governor()
        with patches[0], patches[1], \
             mock.patch.object(gov, "_local_disk_io_busy", return_value=False), \
             gov.mark_fetch_active():
            self.assertFalse(gov.can_start_cold_migration())

    def test_cold_migration_blocks_during_pack(self):
        gov, patches = self._governor()
        with patches[0], patches[1], \
             mock.patch.object(gov, "_local_disk_io_busy", return_value=False), \
             gov.mark_pack_active():
            self.assertFalse(gov.can_start_cold_migration())

    def test_cold_migration_blocks_during_db_sync(self):
        gov, patches = self._governor()
        with patches[0], patches[1], \
             mock.patch.object(gov, "_local_disk_io_busy", return_value=False), \
             gov.mark_db_sync_active():
            self.assertFalse(gov.can_start_cold_migration())

    def test_cold_migration_blocks_during_cleanup(self):
        gov, patches = self._governor()
        with patches[0], patches[1], \
             mock.patch.object(gov, "_local_disk_io_busy", return_value=False), \
             gov.mark_cleanup_active():
            self.assertFalse(gov.can_start_cold_migration())

    def test_cold_migration_blocks_unsafe_ram(self):
        gov, patches = self._governor(vm=_vm(percent=75))
        with patches[0], patches[1], \
             mock.patch.object(gov, "_local_disk_io_busy", return_value=False):
            self.assertFalse(gov.can_start_cold_migration())

    def test_cold_migration_blocks_busy_local_disk_io(self):
        gov, patches = self._governor()
        with patches[0], patches[1], \
             mock.patch.object(gov, "_local_disk_io_busy", return_value=True):
            self.assertFalse(gov.can_start_cold_migration())


class TapeGateAsymmetryTests(unittest.TestCase):
    """Regression coverage for ResourceGovernor._tape_blocks(action).

    A *pending* tape write must only block new stage STARTS (producer side);
    an *active* tape write must also pause mid-stage "continue" checkpoints.
    Blocking "continue" on a merely pending write created a circular wait:
    the producer (fetch/pack) never reaches a point where it can drain, so
    the tape's own start gate (which waits for fetch/pack_active to clear)
    never opens either. See src/resource_governor.py _tape_blocks docstring.
    """

    def _governor(self, cfg=None, vm=None, disk=None):
        cfg = cfg or _cfg()
        gov = ResourceGovernor(cfg, staging_dir=cfg.staging_dir,
                               sleep_seconds=0.01)
        patches = [
            mock.patch("src.resource_governor.psutil.virtual_memory",
                       return_value=vm or _vm()),
            mock.patch("src.resource_governor.shutil.disk_usage",
                       return_value=disk or _disk()),
        ]
        return gov, patches

    def _relaxed_cfg(self, **overrides):
        data = dict(
            ram_hard_limit_pct=100,
            governor_fetch_min_free_floor_gb=0.5,
            governor_fetch_target_free_ram_gb=0.5,
            governor_tape_min_free_ram_gb=0.5,
        )
        data.update(overrides)
        return _cfg(**data)

    def test_pending_tape_write_blocks_fetch_start_but_not_continue(self):
        gov, patches = self._governor(cfg=self._relaxed_cfg())
        with patches[0], patches[1]:
            gov.tape_write_pending = True
            start = gov.decision("fetch", "start")
            cont = gov.decision("fetch", "continue")
        self.assertFalse(start.allowed)
        self.assertIn("tape_active", start.reasons)
        self.assertTrue(cont.allowed, cont.reasons)

    def test_active_tape_write_blocks_fetch_continue(self):
        gov, patches = self._governor(cfg=self._relaxed_cfg())
        with patches[0], patches[1]:
            gov.tape_write_active = True
            cont = gov.decision("fetch", "continue")
        self.assertFalse(cont.allowed)
        self.assertIn("tape_active", cont.reasons)

    def test_pending_tape_write_blocks_pack_start_but_not_continue(self):
        gov, patches = self._governor(cfg=self._relaxed_cfg())
        with patches[0], patches[1]:
            gov.tape_write_pending = True
            start = gov.decision("pack", "start")
            cont = gov.decision("pack", "continue")
        self.assertFalse(start.allowed)
        self.assertIn("tape_active", start.reasons)
        self.assertTrue(cont.allowed, cont.reasons)

    def test_active_tape_write_blocks_pack_continue(self):
        gov, patches = self._governor(cfg=self._relaxed_cfg())
        with patches[0], patches[1]:
            gov.tape_write_active = True
            cont = gov.decision("pack", "continue")
        self.assertFalse(cont.allowed)
        self.assertIn("tape_active", cont.reasons)

    def test_no_deadlock_fetch_active_and_tape_pending_breaks_the_cycle(self):
        gov, patches = self._governor(cfg=self._relaxed_cfg())
        with patches[0], patches[1]:
            gov.fetch_active = True
            gov.tape_write_pending = True
            tape_start = gov.decision("tape", "start")
            fetch_cont = gov.decision("fetch", "continue")
        # The tape write cannot start while fetch is actively running...
        self.assertFalse(tape_start.allowed)
        self.assertIn("heavy_stage_active", tape_start.reasons)
        # ...but fetch's own mid-stage checkpoints are NOT blocked by the
        # pending tape write, so fetch can finish and clear fetch_active,
        # which is exactly what lets the tape's heavy_stage_active gate open.
        self.assertTrue(fetch_cont.allowed, fetch_cont.reasons)

    def test_tape_ram_reserve_applies_only_when_tape_write_is_active(self):
        low_available = int(0.2 * 1024**3)  # below governor_tape_min_free_ram_gb=0.5
        vm = _vm(percent=10, available=low_available, total=64 * 1024**3)

        gov_active, patches_active = self._governor(
            cfg=self._relaxed_cfg(), vm=vm)
        with patches_active[0], patches_active[1]:
            gov_active.tape_write_active = True
            active_cont = gov_active.decision("fetch", "continue")
        self.assertIn("tape_ram_reserve", active_cont.reasons)

        gov_pending, patches_pending = self._governor(
            cfg=self._relaxed_cfg(), vm=vm)
        with patches_pending[0], patches_pending[1]:
            gov_pending.tape_write_pending = True
            pending_cont = gov_pending.decision("fetch", "continue")
        self.assertNotIn("tape_ram_reserve", pending_cont.reasons)


class _FakeDB:
    def tape_exists(self, tape_label):
        return True

    def bulk_upsert_files(self, records, update_existing=True):
        list(records)
        return {"inserted": 0, "updated": 0, "skipped": 0}

    def recalculate_tape_used_space(self, tape_label):
        return 0


class TapeWriteGovernorLifecycleTests(unittest.TestCase):
    _SUCCESS = """
    Files : 1 1 0 0 0 0
    Bytes : 1 1 0 0 0 0
    Speed : 1048576 Bytes/Sec.
    Times : 0:00:01 0:00:01
    """
    _FAIL = """
    Files : 1 0 0 0 1 0
    Bytes : 1 0 0 0 0 0
    Speed : 1048576 Bytes/Sec.
    Times : 0:00:01 0:00:01
    """

    def _run_backup(self, stdout, returncode):
        with tempfile.TemporaryDirectory() as tmp:
            source = os.path.join(tmp, "source")
            tape = os.path.join(tmp, "tape")
            os.makedirs(source)
            os.makedirs(tape)
            with open(os.path.join(source, "file.bin"), "wb") as f:
                f.write(b"x")
            cfg = _cfg(staging_dir=tmp)
            gov = ResourceGovernor(cfg, staging_dir=tmp, sleep_seconds=0.01)
            backup = LTOBackup(_FakeDB(), "", governor=gov, log_dir=tmp)
            backup.eject_tape = lambda _drive: None
            result = SimpleNamespace(
                stdout=stdout, stderr="", returncode=returncode)
            with mock.patch("src.backup._ensure_lto_drive_ready",
                            return_value=True), \
                 mock.patch("src.backup._run_robocopy_tuned",
                            return_value=result), \
                 mock.patch.object(backup, "_write_backup_log",
                                   return_value=""):
                if returncode >= 8:
                    with self.assertRaises(RuntimeError):
                        backup.run(source, tape, "T1")
                else:
                    backup.run(source, tape, "T1")
            return gov

    def test_tape_write_active_clears_on_success(self):
        gov = self._run_backup(self._SUCCESS, 1)
        self.assertFalse(gov.tape_write_active)

    def test_tape_write_active_clears_on_failure(self):
        gov = self._run_backup(self._FAIL, 8)
        self.assertFalse(gov.tape_write_active)


if __name__ == "__main__":
    unittest.main()
