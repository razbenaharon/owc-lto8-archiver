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


def _vm(percent=20, available=64 * 1024**3):
    return SimpleNamespace(percent=percent, available=available)


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
