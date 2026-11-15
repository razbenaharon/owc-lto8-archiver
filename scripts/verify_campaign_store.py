"""Verify a local campaign container store against its published receipts.

The localization campaign wrote each archive chunk as one or more stored-TAR
containers plus a ``receipt.json`` recording, per container, the exact byte
size and SHA-256. Those receipts are the integrity anchor: for containers
whose only copy currently lives on one unstable external drive, the receipt is
the sole way to tell an intact container from a silently truncated or
bit-rotted one, and it does so without reading tape or re-fetching the source.

Two modes:

* ``structure`` — metadata only. Proves every receipted container is present
  at its recorded size and that the receipt's own totals are self-consistent.
  Catches truncation and missing files instantly, reading no file bodies.
* ``full`` — additionally streams every container through SHA-256. Catches
  silent corruption. Reads strictly one file at a time; the source drive must
  not be hammered with parallel I/O.

A drive that disappears mid-run is an expected outcome here, not a crash: the
affected container is recorded ``io_error`` and the run continues, so one
disconnect never costs the whole verification pass.

"OK" always means *something was verified*. A receipt that lists no
containers, or lacks a field the published schema
(``examples/container-receipt.schema.json``) requires, FAILS: a verifier that
passes when there is nothing to check proves nothing. The run as a whole exits
non-zero if it verified no container at all.

TAR sidecars are receipted too, but they normally live under the local
manifest root on the workstation, not in the container store. A sidecar found
under ``--root`` is verified; one that is not there is reported as *not
checked* (never as OK). Pass ``--sidecar-root`` to make every receipted sidecar
mandatory.
"""
import argparse
import errno
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone

BLOCK_SIZE = 8 * 1024 * 1024
CHUNK_DIR_RE = re.compile(r"^chunk_(\d+)$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

# Mirrors the "required" lists of examples/container-receipt.schema.json.
REQUIRED_RECEIPT_FIELDS = ("schema_version", "chunk_index", "session_id",
                           "containers", "expected_totals", "observed_totals")
REQUIRED_CONTAINER_FIELDS = ("container_ordinal", "tar_locator", "tar_size",
                             "tar_sha256", "member_count", "logical_bytes")
REQUIRED_TOTALS_FIELDS = ("member_count", "logical_bytes")

# Windows surfaces a yanked removable volume as WinError 21 (device not ready)
# or 1117 (I/O device error); POSIX uses ENODEV/ENOMEDIUM/EIO. Any of them
# means "the drive went away", never "the data is bad".
_IO_ERRNOS = {errno.EIO, errno.ENODEV, getattr(errno, "ENOMEDIUM", -1)}
_IO_WINERRORS = {21, 1117}


def _is_drive_io_error(exc, root=None):
    """True when this failure is the medium disappearing, not bad data.

    Errno alone is not enough. A volume that drops while still mounted keeps
    serving cached directory metadata, so ``os.path.getsize`` succeeds and the
    subsequent read fails as EACCES rather than any device errno — which would
    otherwise be recorded as a data failure and read as corruption. Whenever
    ``root`` is supplied, re-probe it: if the store root itself can no longer
    be listed, the drive is gone and every failure under it is an I/O error
    whatever errno claims.
    """
    if (getattr(exc, "errno", None) in _IO_ERRNOS
            or getattr(exc, "winerror", None) in _IO_WINERRORS):
        return True
    if root is None:
        return False
    try:
        os.listdir(root)
    except OSError:
        return True
    return False


def _now():
    return datetime.now(timezone.utc).isoformat()


def _human(num_bytes):
    value = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024


def discover_chunks(root, only=None):
    """Return [(chunk_index, dirpath)] sorted by index."""
    found = []
    for name in os.listdir(root):
        match = CHUNK_DIR_RE.match(name)
        path = os.path.join(root, name)
        if match and os.path.isdir(path):
            index = int(match.group(1))
            if only is None or index in only:
                found.append((index, path))
    return sorted(found)


def _resolve_container(root, chunk_dir, locator, name):
    """Receipt locators are root-relative; fall back to the chunk directory."""
    candidate = os.path.join(root, *str(locator or "").split("/"))
    if os.path.isfile(candidate):
        return candidate
    return os.path.join(chunk_dir, name)


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(BLOCK_SIZE), b""):
            digest.update(block)
    return digest.hexdigest()


def _is_count(value):
    """A non-negative integer (JSON booleans are not counts)."""
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _receipt_shape_problems(receipt):
    """Why this receipt cannot anchor a verification, or []."""
    if not isinstance(receipt, dict):
        return ["receipt is not a JSON object"]
    problems = []
    missing = [name for name in REQUIRED_RECEIPT_FIELDS
               if receipt.get(name) is None]
    if missing:
        problems.append(
            "receipt is missing required field(s): " + ", ".join(missing))
    for name in ("expected_totals", "observed_totals"):
        totals = receipt.get(name)
        if totals is None:
            continue
        if not isinstance(totals, dict) or not all(
                _is_count(totals.get(key)) for key in REQUIRED_TOTALS_FIELDS):
            problems.append(
                f"{name} must carry integer member_count and logical_bytes")
    containers = receipt.get("containers")
    if containers is not None and (
            not isinstance(containers, list) or not containers):
        problems.append(
            "receipt lists zero containers; there is nothing to verify")
    return problems


def _container_shape_problems(container):
    """Why this container entry cannot be verified, or []."""
    if not isinstance(container, dict):
        return ["container entry is not a JSON object"]
    problems = []
    missing = [name for name in REQUIRED_CONTAINER_FIELDS
               if container.get(name) in (None, "")]
    if missing:
        problems.append("missing required field(s): " + ", ".join(missing))
    for name in ("container_ordinal", "tar_size", "member_count",
                 "logical_bytes"):
        value = container.get(name)
        if value is not None and not _is_count(value):
            problems.append(f"{name} is not a non-negative integer")
    for name in ("tar_sha256", "sidecar_sha256"):
        value = container.get(name)
        if value not in (None, "") and not (
                isinstance(value, str) and SHA256_RE.match(value)):
            problems.append(f"{name} is not a SHA-256 hex digest")
    return problems


def _resolve_sidecar(root, sidecar_root, locator):
    """(path, required). Sidecar locators are relative to a metadata root."""
    parts = [part for part in str(locator).replace("\\", "/").split("/")
             if part not in ("", ".")]
    if not parts or ".." in parts or ":" in str(locator):
        return None, True
    if sidecar_root:
        return os.path.join(sidecar_root, *parts), True
    return os.path.join(root, *parts), False


def _check_sidecar(root, sidecar_root, container, *, full):
    """Verify a receipted sidecar. Returns (state, detail).

    ``state`` is ``verified``, ``not_checked`` (not in this store and not made
    mandatory), ``absent`` (the receipt names none), ``fail`` or ``io_error``.
    """
    locator = container.get("sidecar_locator")
    if not locator:
        return "absent", ""
    path, required = _resolve_sidecar(root, sidecar_root, locator)
    if path is None:
        return "fail", f"sidecar locator is not a safe relative path: {locator!r}"
    try:
        actual_size = os.path.getsize(path)
    except OSError as exc:
        if _is_drive_io_error(exc, sidecar_root or root):
            return "io_error", f"sidecar unreadable: {exc}"
        if not required and isinstance(exc, FileNotFoundError):
            return "not_checked", ""
        return "fail", f"sidecar missing or unreadable: {exc}"
    expected_size = container.get("sidecar_size")
    if not _is_count(expected_size):
        return "fail", "sidecar present but the receipt records no sidecar_size"
    if actual_size != expected_size:
        return "fail", f"sidecar size {actual_size} != receipt {expected_size}"
    if full:
        expected_sha = container.get("sidecar_sha256")
        if not expected_sha:
            return "fail", "sidecar present but the receipt records no sidecar_sha256"
        try:
            if _sha256(path) != expected_sha:
                return "fail", "sidecar SHA-256 mismatch"
        except OSError as exc:
            state = ("io_error" if _is_drive_io_error(exc, sidecar_root or root)
                     else "fail")
            return state, f"sidecar read failed: {exc}"
    return "verified", ""


def _check_receipt_totals(receipt):
    """Self-consistency of the receipt, independent of what is on disk."""
    problems = []
    expected = receipt.get("expected_totals") or {}
    observed = receipt.get("observed_totals") or {}
    if expected != observed:
        problems.append("expected_totals != observed_totals")
    if receipt.get("missing_members"):
        problems.append(
            f"{len(receipt['missing_members'])} missing member(s) recorded")
    # Malformed entries are reported per container; here they simply do not
    # contribute, so the sums below disagree with the totals and say so.
    containers = [c for c in receipt.get("containers") or []
                  if isinstance(c, dict)]
    summed_bytes = sum(c["logical_bytes"] for c in containers
                       if _is_count(c.get("logical_bytes")))
    summed_members = sum(c["member_count"] for c in containers
                         if _is_count(c.get("member_count")))
    if expected.get("logical_bytes") is not None and (
            summed_bytes != int(expected["logical_bytes"])):
        problems.append("container logical_bytes do not sum to expected_totals")
    if expected.get("member_count") is not None and (
            summed_members != int(expected["member_count"])):
        problems.append("container member_count does not sum to expected_totals")
    return problems


def verify_chunk(root, index, chunk_dir, *, full, already_ok,
                 sidecar_root=None):
    """Verify one chunk directory. Never raises for data or drive problems.

    ``ok`` is returned only when at least one receipted container was actually
    checked and every check passed.
    """
    result = {"chunk_index": index, "dir": chunk_dir, "status": "ok",
              "containers": [], "detail": "", "bytes_verified": 0,
              "sidecars_verified": 0, "sidecars_not_checked": 0}
    receipt_path = os.path.join(chunk_dir, "receipt.json")
    try:
        with open(receipt_path, encoding="utf-8") as handle:
            receipt = json.load(handle)
    except FileNotFoundError:
        result.update(status="no_receipt",
                      detail="receipt.json absent; container is unverifiable")
        return result
    except OSError as exc:
        result.update(status="io_error" if _is_drive_io_error(exc, root) else "fail",
                      detail=f"receipt unreadable: {exc}")
        return result
    except json.JSONDecodeError as exc:
        result.update(status="fail", detail=f"receipt is not valid JSON: {exc}")
        return result

    if not isinstance(receipt, dict):
        result.update(status="fail", detail="receipt is not a JSON object")
        return result
    if receipt.get("schema_version") != 1:
        result.update(status="fail",
                      detail=f"unsupported schema_version {receipt.get('schema_version')!r}")
        return result

    shape = _receipt_shape_problems(receipt)
    if shape:
        # Without the required fields nothing below would be checking what it
        # appears to check, so stop rather than report a partial "pass".
        result.update(status="fail", detail="; ".join(shape))
        return result

    problems = _check_receipt_totals(receipt)
    listed = set()
    for position, container in enumerate(receipt["containers"]):
        entry = {"name": f"containers[{position}]", "size_ok": False,
                 "sha_ok": None, "sidecar": "absent", "status": "ok",
                 "detail": ""}
        container_shape = _container_shape_problems(container)
        if container_shape:
            entry.update(status="fail", detail="; ".join(container_shape))
            result["containers"].append(entry)
            problems.append(f"{entry['name']}: {entry['detail']}")
            continue
        locator = container["tar_locator"]
        name = os.path.basename(locator) or (
            f"container_{int(container['container_ordinal']):04d}.tar")
        entry["name"] = name
        listed.add(name)
        path = _resolve_container(root, chunk_dir, locator, name)
        try:
            info = os.stat(path)
        except OSError as exc:
            entry.update(status="io_error" if _is_drive_io_error(exc, root) else "fail",
                         detail=f"missing or unreadable: {exc}")
            result["containers"].append(entry)
            problems.append(f"{name}: {entry['detail']}")
            continue
        actual_size = info.st_size
        expected_size = container["tar_size"]
        entry["size_ok"] = actual_size == expected_size
        if not entry["size_ok"]:
            entry.update(status="fail",
                         detail=f"size {actual_size} != receipt {expected_size}")
            problems.append(f"{name}: size mismatch")
            result["containers"].append(entry)
            continue
        # What a later --resume may rely on: this exact file state was hashed
        # and matched this exact receipt checksum.
        evidence = {"size": actual_size, "mtime_ns": info.st_mtime_ns,
                    "tar_sha256": container["tar_sha256"]}
        if full and _resume_evidence_matches(already_ok, name, evidence):
            entry["sha_ok"] = True
            entry["detail"] = ("sha verified in an earlier run (--resume); "
                               "size, mtime and receipt checksum unchanged")
            entry.update(evidence)
        elif full:
            try:
                entry["sha_ok"] = _sha256(path) == container.get("tar_sha256")
                if entry["sha_ok"]:
                    after = os.stat(path)
                    if (after.st_size, after.st_mtime_ns) == (
                            actual_size, info.st_mtime_ns):
                        entry.update(evidence)
            except OSError as exc:
                entry.update(status="io_error" if _is_drive_io_error(exc, root) else "fail",
                             detail=f"read failed: {exc}")
                result["containers"].append(entry)
                problems.append(f"{name}: {entry['detail']}")
                continue
            if not entry["sha_ok"]:
                entry.update(status="fail", detail="SHA-256 mismatch")
                problems.append(f"{name}: SHA-256 mismatch")
        sidecar_state, sidecar_detail = _check_sidecar(
            root, sidecar_root, container, full=full)
        entry["sidecar"] = sidecar_state
        if sidecar_state == "verified":
            result["sidecars_verified"] += 1
        elif sidecar_state == "not_checked":
            result["sidecars_not_checked"] += 1
        elif sidecar_state in ("fail", "io_error"):
            entry.update(status=sidecar_state, detail="; ".join(
                part for part in (entry["detail"], sidecar_detail) if part))
            problems.append(f"{name}: {sidecar_detail}")
        if entry["status"] == "ok":
            result["bytes_verified"] += actual_size
        result["containers"].append(entry)

    stray = sorted(
        name for name in os.listdir(chunk_dir)
        if name.lower().endswith(".tar") and name not in listed)
    if stray:
        problems.append(f"unreceipted container(s) present: {', '.join(stray)}")

    if any(c["status"] == "io_error" for c in result["containers"]):
        result["status"] = "io_error"
    elif problems:
        result["status"] = "fail"
    result["detail"] = "; ".join(problems)
    return result


_RESUME_EVIDENCE_KEYS = ("size", "mtime_ns", "tar_sha256")


def _resume_evidence_matches(already_ok, name, evidence):
    """True only if an earlier report hashed THIS file state against THIS
    receipt checksum.

    A name alone proves nothing: the bytes can change at the same size, and
    the receipt can change under an unchanged file. Anything short of an exact
    match on size, mtime and receipt checksum - including an older report that
    recorded no evidence - means "hash it again".
    """
    if not isinstance(already_ok, dict):
        return False
    recorded = already_ok.get(name)
    return (isinstance(recorded, dict)
            and all(recorded.get(key) is not None
                    and recorded.get(key) == evidence[key]
                    for key in _RESUME_EVIDENCE_KEYS))


def _resume_index(report_path):
    """{chunk_index: {container name: recorded evidence}} from a report.

    Only containers the earlier run sha-verified are listed; the evidence is
    whatever that report recorded (possibly nothing, for an older report).
    """
    already = {}
    try:
        with open(report_path, encoding="utf-8") as handle:
            previous = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return already
    for chunk in previous.get("chunks") or []:
        names = {c["name"]: {key: c.get(key) for key in _RESUME_EVIDENCE_KEYS}
                 for c in chunk.get("containers") or []
                 if c.get("sha_ok") is True}
        if names:
            already[int(chunk["chunk_index"])] = names
    return already


def _write_report(path, payload):
    if not path:
        return
    temporary = path + ".part"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    os.replace(temporary, path)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", required=True,
                        help="Campaign root holding chunk_NNNNNN directories.")
    parser.add_argument("--mode", choices=("structure", "full"),
                        default="structure",
                        help="structure: metadata only; full: also SHA-256.")
    parser.add_argument("--report", help="Write a JSON report to this path.")
    parser.add_argument("--chunk", type=int, action="append",
                        help="Limit to this chunk index (repeatable).")
    parser.add_argument("--resume", action="store_true",
                        help="Skip containers already sha-verified in --report.")
    parser.add_argument("--sidecar-root",
                        help="Metadata root the receipts' sidecar_locator is "
                             "relative to. When given, every receipted sidecar "
                             "must be present and match; otherwise sidecars "
                             "not found under --root are reported as not "
                             "checked.")
    args = parser.parse_args(argv)

    root = os.path.abspath(args.root)
    sidecar_root = (os.path.abspath(args.sidecar_root)
                    if args.sidecar_root else None)
    full = args.mode == "full"
    chunks = discover_chunks(root, set(args.chunk) if args.chunk else None)
    if not chunks:
        print(f"No chunk directories under {root}", file=sys.stderr)
        return 2
    already = _resume_index(args.report) if (args.resume and args.report) else {}

    results, interrupted = [], False
    try:
        for position, (index, chunk_dir) in enumerate(chunks, start=1):
            outcome = verify_chunk(root, index, chunk_dir, full=full,
                                   already_ok=already.get(index, set()),
                                   sidecar_root=sidecar_root)
            results.append(outcome)
            label = {"ok": "OK", "fail": "FAIL", "no_receipt": "NO-RECEIPT",
                     "io_error": "IO-ERROR"}[outcome["status"]]
            unchecked = outcome["sidecars_not_checked"]
            note = (f"  [{unchecked} sidecar(s) not checked]"
                    if unchecked else "")
            print(f"[{position:3d}/{len(chunks)}] {os.path.basename(chunk_dir)}"
                  f"  {label:<10} {len(outcome['containers'])} container(s)"
                  f"  {_human(outcome['bytes_verified'])}{note}"
                  f"{('  ' + outcome['detail']) if outcome['detail'] else ''}",
                  flush=True)
    except KeyboardInterrupt:
        interrupted = True
        print("\nInterrupted; writing partial report.", flush=True)

    tally = {state: sum(1 for r in results if r["status"] == state)
             for state in ("ok", "fail", "no_receipt", "io_error")}
    summary = {
        "chunks_examined": len(results),
        "chunks_total": len(chunks),
        **tally,
        "containers": sum(len(r["containers"]) for r in results),
        "containers_verified": sum(
            1 for r in results for c in r["containers"]
            if c["status"] == "ok"),
        "bytes_verified": sum(r["bytes_verified"] for r in results),
        "sidecars_verified": sum(r["sidecars_verified"] for r in results),
        "sidecars_not_checked": sum(
            r["sidecars_not_checked"] for r in results),
        "sha256_checked": full,
        "interrupted": interrupted,
    }
    payload = {"root": root, "sidecar_root": sidecar_root, "mode": args.mode,
               "generated_at": _now(), "summary": summary, "chunks": results}
    _write_report(args.report, payload)

    print(f"\n{args.mode} verification of {root}")
    print(f"  ok={tally['ok']}  fail={tally['fail']}  "
          f"no_receipt={tally['no_receipt']}  io_error={tally['io_error']}"
          f"  ({summary['containers']} containers, "
          f"{_human(summary['bytes_verified'])} verified)")
    for outcome in results:
        if outcome["status"] != "ok":
            print(f"  - {os.path.basename(outcome['dir'])}: "
                  f"{outcome['status']}: {outcome['detail']}")
    # Say what an "ok" above does NOT cover.
    if not full:
        print("  note: structure mode compares sizes only - SHA-256 was NOT "
              "checked; silent corruption would pass. Use --mode full.")
    print(f"  sidecars: {summary['sidecars_verified']} verified, "
          f"{summary['sidecars_not_checked']} not checked"
          + (" (not found under --root; pass --sidecar-root to require them)"
             if summary["sidecars_not_checked"] else ""))
    if summary["containers_verified"] == 0:
        print("  NOTHING WAS VERIFIED: no receipted container passed a check.",
              file=sys.stderr)
        return 1
    return 0 if (tally["ok"] == len(results) and not interrupted) else 1


if __name__ == "__main__":
    sys.exit(main())
