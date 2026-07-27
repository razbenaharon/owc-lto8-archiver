"""Post-remount go/no-go check. Read-only: never moves tape, never writes.

Run this after cleaning the drive and remounting the cartridge, BEFORE resuming
a session. It answers, in order, the five questions that decide whether a resume
is safe, and prints a single GO / NO-GO verdict.

  1. Is the drive letter still what config.ini says?  The LTFS mount letter is
     not stable across remounts (it was E:, became Z:), and Get-Volume cannot
     see LTFS, so this has to be checked every time.
  2. Which cartridge is actually loaded?  A resumed session takes its tape label
     from the database and would otherwise catalog Tape_03 writes under whatever
     the row says.
  3. Did the cartridge come back frozen?  Event 11333 on mount means the PWE bit
     is set in the cartridge MAM and the volume mounts read-only for good --
     that is the cartridge, not the mount, and no amount of remounting fixes it.
  4. Is the mount time@5?  Anything else risks losing the index on an
     interruption, which is what cost ~126 GB on 2026-07-15.
  5. Is the drive reporting faults on THIS mount?  Scoped to the new mount start
     so the pre-cleaning history cannot mask a fresh fault -- or block a clean
     one.

Exit code 0 = GO, 1 = NO-GO, 2 = could not determine (treat as NO-GO).

    python scripts/post_remount_check.py
    python scripts/post_remount_check.py --expect-label Tape_03
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import ConfigManager
from src.constants import PROJECT_ROOT
from src.ltfs import get_volume_label
from src.windows_update_guard import (
    ltfs_current_mount_status,
    ltfs_media_health,
)

BASELINE_PATH = os.path.join("backup_logs", "pre_clean_baseline.json")

#: Emitted at mount time when the cartridge itself carries the permanent write
#: error. Distinct from 12045, which is only the mount dropping to read-only.
FROZEN_CARTRIDGE_EVENT = 11333


def _load_baseline():
    try:
        with open(BASELINE_PATH, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def _check_drive_letter(cfg, findings):
    drive = cfg.lto_drive
    present = os.path.exists(drive)
    findings.append({
        "check": "drive letter",
        "ok": present,
        "detail": f"config.ini lto_drive={drive} exists={present}",
        "note": None if present else
                "Mount letter moved. Fix [HARDWARE] lto_drive before resuming.",
    })
    return present


def _check_cartridge(cfg, expect_label, findings):
    label = get_volume_label(cfg.lto_drive)
    if expect_label is None:
        findings.append({
            "check": "cartridge identity",
            "ok": label is not None,
            "detail": f"loaded volume label={label!r} (no --expect-label given)",
            "note": "Pass --expect-label to have this verified, not just read.",
        })
        return label is not None
    ok = (label == expect_label)
    findings.append({
        "check": "cartridge identity",
        "ok": ok,
        "detail": f"loaded={label!r} expected={expect_label!r}",
        "note": None if ok else
                "WRONG CARTRIDGE. Resuming would catalog writes under the "
                "session's recorded tape label, silently.",
    })
    return ok


def _check_mount(findings):
    mount = ltfs_current_mount_status()
    if not mount.get("determinate"):
        findings.append({
            "check": "mount time@5",
            "ok": None,
            "detail": f"indeterminate: {mount.get('error')}",
            "note": "Cannot prove sync mode. Treat as NO-GO.",
        })
        return None, mount
    ok = bool(mount.get("ok")) and bool(mount.get("bound_to_current"))
    findings.append({
        "check": "mount time@5",
        "ok": ok,
        "detail": (f"sync={mount.get('sync_type')}@{mount.get('sync_seconds')}s "
                   f"started={mount.get('mount_started_at')} "
                   f"bound_to_current={mount.get('bound_to_current')}"),
        "note": None if ok else (mount.get("reason") or
                                 "Mount is not a verified time@5 mount."),
    })
    return ok, mount


def _check_media_health(mount, baseline, findings):
    started = mount.get("mount_started_at")
    if not started:
        findings.append({
            "check": "drive/medium health",
            "ok": None,
            "detail": "no mount start time; health cannot be scoped",
            "note": "Unscoped health mixes in the previous cartridge. NO-GO.",
        })
        return None, {}
    previous = baseline.get("pre_clean_mount_started_at")
    if previous and started == previous:
        findings.append({
            "check": "drive/medium health",
            "ok": False,
            "detail": f"mount start {started} is unchanged from the "
                      "pre-cleaning baseline",
            "note": "The volume was never actually remounted -- this is still "
                    "the read-only mount that failed. Remount before resuming.",
        })
        return False, {}
    health = ltfs_media_health(since_iso=started)
    if not health.get("determinate"):
        findings.append({
            "check": "drive/medium health",
            "ok": None,
            "detail": f"indeterminate: {health.get('error')}",
            "note": "Fail closed: treat as NO-GO.",
        })
        return None, health
    fatal = health.get("fatal") or []
    degraded = health.get("degraded") or []
    frozen = [e for e in fatal if e.get("id") == FROZEN_CARTRIDGE_EVENT]
    note = None
    if frozen:
        note = ("Event 11333: the CARTRIDGE carries the permanent write error, "
                "not just the mount. It is read-only for good -- retire it "
                "(tapes.status='full') and load a different one.")
    elif fatal or degraded:
        note = ("Drive is still reporting faults after cleaning. Do not "
                "resume; escalate to IBM with the .dmp files.")
    findings.append({
        "check": "drive/medium health",
        "ok": bool(health.get("ok")),
        "detail": (f"since={started} fatal={len(fatal)} degraded={len(degraded)}"
                   + ("" if not (fatal or degraded) else
                      "; newest: " + "; ".join(
                          f"[{e['id']}] {e['meaning']}"
                          for e in (fatal or degraded)[:3]))),
        "note": note,
    })
    return bool(health.get("ok")), health


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Read-only go/no-go check after cleaning and remounting.")
    parser.add_argument("--expect-label",
                        help="Volume label the loaded cartridge must have.")
    args = parser.parse_args(argv)
    os.chdir(PROJECT_ROOT)

    cfg = ConfigManager()
    baseline = _load_baseline()
    findings = []

    _check_drive_letter(cfg, findings)
    _check_cartridge(cfg, args.expect_label, findings)
    mount_ok, mount = _check_mount(findings)
    if mount_ok is not None:
        _check_media_health(mount, baseline, findings)

    print("\n=== post-remount check ===")
    if baseline:
        print(f"baseline mount was {baseline.get('pre_clean_mount_started_at')}"
              f"  drive={baseline.get('drive_serial')}")
    verdict = 0
    for item in findings:
        mark = {True: "PASS", False: "FAIL", None: "UNKNOWN"}[item["ok"]]
        print(f"  [{mark:7}] {item['check']}: {item['detail']}")
        if item["note"]:
            print(f"            -> {item['note']}")
        if item["ok"] is False:
            verdict = 1
        elif item["ok"] is None and verdict == 0:
            verdict = 2

    print()
    if verdict == 0:
        print("GO - safe to resume: python run.py remote-archive --resume")
    elif verdict == 2:
        print("NO-GO - a check could not be determined. Fail closed.")
    else:
        print("NO-GO - do not resume. See the FAIL lines above.")
    return verdict


if __name__ == "__main__":
    raise SystemExit(main())
