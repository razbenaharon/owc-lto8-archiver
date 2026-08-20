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
    containers = receipt.get("containers") or []
    summed_bytes = sum(int(c.get("logical_bytes") or 0) for c in containers)
    summed_members = sum(int(c.get("member_count") or 0) for c in containers)
    if expected.get("logical_bytes") is not None and (
            summed_bytes != int(expected["logical_bytes"])):
        problems.append("container logical_bytes do not sum to expected_totals")
    if expected.get("member_count") is not None and (
            summed_members != int(expected["member_count"])):
        problems.append("container member_count does not sum to expected_totals")
    return problems


def verify_chunk(root, index, chunk_dir, *, full, already_ok):
    """Verify one chunk directory. Never raises for data or drive problems."""
    result = {"chunk_index": index, "dir": chunk_dir, "status": "ok",
              "containers": [], "detail": "", "bytes_verified": 0}
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

    if int(receipt.get("schema_version") or 0) != 1:
        result.update(status="fail",
                      detail=f"unsupported schema_version {receipt.get('schema_version')!r}")
        return result

    problems = _check_receipt_totals(receipt)
    listed = set()
    for container in receipt.get("containers") or []:
        locator = container.get("tar_locator") or ""
        name = os.path.basename(locator) or (
            f"container_{int(container.get('container_ordinal') or 0):04d}.tar")
        listed.add(name)
        path = _resolve_container(root, chunk_dir, locator, name)
        entry = {"name": name, "size_ok": False, "sha_ok": None,
                 "status": "ok", "detail": ""}
        try:
            actual_size = os.path.getsize(path)
        except OSError as exc:
            entry.update(status="io_error" if _is_drive_io_error(exc, root) else "fail",
                         detail=f"missing or unreadable: {exc}")
            result["containers"].append(entry)
            problems.append(f"{name}: {entry['detail']}")
            continue
        expected_size = int(container.get("tar_size") or -1)
        entry["size_ok"] = actual_size == expected_size
        if not entry["size_ok"]:
            entry.update(status="fail",
                         detail=f"size {actual_size} != receipt {expected_size}")
            problems.append(f"{name}: size mismatch")
            result["containers"].append(entry)
            continue
        if full and name in already_ok:
            entry["sha_ok"] = True
            entry["detail"] = "sha verified in an earlier run (--resume)"
        elif full:
            try:
                entry["sha_ok"] = _sha256(path) == container.get("tar_sha256")
            except OSError as exc:
                entry.update(status="io_error" if _is_drive_io_error(exc, root) else "fail",
                             detail=f"read failed: {exc}")
                result["containers"].append(entry)
                problems.append(f"{name}: {entry['detail']}")
                continue
            if not entry["sha_ok"]:
                entry.update(status="fail", detail="SHA-256 mismatch")
                problems.append(f"{name}: SHA-256 mismatch")
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


def _resume_index(report_path):
    """{chunk_index: {container names already sha-verified}} from a report."""
    already = {}
    try:
        with open(report_path, encoding="utf-8") as handle:
            previous = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return already
    for chunk in previous.get("chunks") or []:
        names = {c["name"] for c in chunk.get("containers") or []
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
    args = parser.parse_args(argv)

    root = os.path.abspath(args.root)
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
                                   already_ok=already.get(index, set()))
            results.append(outcome)
            label = {"ok": "OK", "fail": "FAIL", "no_receipt": "NO-RECEIPT",
                     "io_error": "IO-ERROR"}[outcome["status"]]
            print(f"[{position:3d}/{len(chunks)}] {os.path.basename(chunk_dir)}"
                  f"  {label:<10} {len(outcome['containers'])} container(s)"
                  f"  {_human(outcome['bytes_verified'])}"
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
        "bytes_verified": sum(r["bytes_verified"] for r in results),
        "interrupted": interrupted,
    }
    payload = {"root": root, "mode": args.mode, "generated_at": _now(),
               "summary": summary, "chunks": results}
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
    return 0 if (tally["ok"] == len(results) and not interrupted) else 1


if __name__ == "__main__":
    sys.exit(main())
