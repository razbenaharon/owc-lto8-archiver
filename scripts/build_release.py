"""Build the release package: one zip of the committed tree, plus checksums.

    python scripts/build_release.py                    # -> dist/
    python scripts/build_release.py --expect-tag v1.0.0-rc.1

Produces, in ``dist/`` (gitignored):

    tape-ledger-<version>.zip    ``git archive`` of HEAD - tracked files
                                       only, under one top-level folder
    SHA256SUMS.txt                     ``<sha256>  <file name>``

There is ONE package for both catalog backends: the backend is a line in
``config.ini``, and the two installation options are the two dependency
files inside the zip (``requirements-core.txt`` / ``requirements-postgres.txt``).

It refuses, before writing anything, when:

* the working tree is not clean (the zip is built from HEAD, so anything
  uncommitted would silently be missing from it - or the checks below would
  be judging files that are not what ships);
* the privacy gate (``scripts/check_public_repo.py``) fails;
* ``--expect-tag`` does not match the version in ``src/__init__.py``.

And it deletes what it built when the zip turns out to contain a local or
private file (config.ini, .env, a database, a log), or to lack a file the
install instructions depend on.

This script only builds files. It does not create tags, push, or publish.
"""
import argparse
import hashlib
import os
import re
import subprocess
import sys
import zipfile

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PACKAGE_NAME = "tape-ledger"
CHECKSUM_FILE = "SHA256SUMS.txt"

#: Must be in the package: both dependency sets, the config template, the
#: entry points and what a first run needs.
REQUIRED_FILES = (
    "LICENSE",
    "README.md",
    "config.example.ini",
    ".env.example",
    "requirements.txt",
    "requirements-core.txt",
    "requirements-postgres.txt",
    "requirements-gui.txt",
    "run.py",
    "inspect_db.py",
    "src/__init__.py",
    "scripts/sql/sqlite/001_schema.sql",
    "scripts/sql/001_postgres_schema.sql",
)

#: Must never be in the package (matched on the lower-cased path inside the
#: zip, without the top-level folder).
FORBIDDEN_PATTERNS = (
    (r"(^|/)config\.ini(\.bak.*)?$", "operator configuration"),
    (r"(^|/)\.env$", "secrets file"),
    (r"(^|/)\.env\.(?!example$)", "secrets file"),
    (r"\.(db|sqlite|sqlite3)(-wal|-shm|-journal)?$", "database"),
    (r"\.(dump|backup|bak)$", "database dump or backup"),
    (r"\.(log|rawlog)$", "log"),
    (r"(^|/)(backup_logs|db_backups|private|dist|venv|\.venv)/",
     "local working directory"),
    (r"(^|/)__pycache__/|\.pyc$", "bytecode cache"),
)

_VERSION = re.compile(r'^__version__\s*=\s*"([^"]+)"\s*$', re.M)
_PEP440 = re.compile(r"^(\d+\.\d+\.\d+)(?:(a|b|rc)(\d+))?$")
_STAGE_WORD = {"a": "alpha", "b": "beta", "rc": "rc"}


class BuildRefused(Exception):
    """The release must not be built. The message says why."""


def read_version(repo=REPO_ROOT):
    """The version from ``src/__init__.py``, read without importing it."""
    with open(os.path.join(repo, "src", "__init__.py"),
              encoding="utf-8") as handle:
        found = _VERSION.findall(handle.read())
    if len(found) != 1:
        raise BuildRefused(
            "src/__init__.py must define __version__ exactly once as a "
            f"plain string; found {found}")
    return found[0]


def tag_for(version):
    """``1.0.0rc1`` -> ``v1.0.0-rc.1``; ``1.0.0`` -> ``v1.0.0``."""
    match = _PEP440.match(version)
    if not match:
        raise BuildRefused(
            f"version {version!r} is not of the form X.Y.Z or X.Y.Z(a|b|rc)N")
    base, stage, number = match.groups()
    if not stage:
        return f"v{base}"
    return f"v{base}-{_STAGE_WORD[stage]}.{number}"


def is_prerelease(version):
    match = _PEP440.match(version)
    return bool(match and match.group(2))


def _git(repo, *args):
    done = subprocess.run(["git", "-C", repo, *args], capture_output=True,
                          text=True, encoding="utf-8", errors="replace")
    if done.returncode != 0:
        raise BuildRefused(f"git {' '.join(args)} failed: "
                           + (done.stderr or done.stdout).strip())
    return done.stdout


def require_clean_tree(repo):
    status = _git(repo, "status", "--porcelain").strip()
    if status:
        lines = status.splitlines()
        shown = "\n  ".join(lines[:15])
        more = f"\n  ... and {len(lines) - 15} more" if len(lines) > 15 else ""
        raise BuildRefused(
            "the working tree is not clean. The package is built from HEAD, "
            "so commit or remove these first:\n  " + shown + more)


def run_privacy_gate(repo):
    gate = os.path.join(repo, "scripts", "check_public_repo.py")
    done = subprocess.run([sys.executable, gate], cwd=repo,
                          capture_output=True, text=True, encoding="utf-8",
                          errors="replace")
    if done.returncode != 0:
        raise BuildRefused("the privacy gate failed:\n"
                           + (done.stdout + done.stderr).strip())
    return done.stdout.strip().splitlines()[-1] if done.stdout.strip() else ""


def inspect_package(zip_path, version):
    """Check one built zip. Returns the list of member files (no folder
    prefix); raises :class:`BuildRefused` on any problem."""
    prefix = f"{PACKAGE_NAME}-{version}/"
    problems = []
    with zipfile.ZipFile(zip_path) as bundle:
        corrupt = bundle.testzip()
        if corrupt:
            raise BuildRefused(f"the zip is corrupt at member {corrupt}")
        names = [info.filename for info in bundle.infolist()
                 if not info.is_dir()]
        outside = [name for name in names if not name.startswith(prefix)]
        if outside:
            problems.append(f"members outside {prefix}: {outside[:5]}")
        members = [name[len(prefix):] for name in names
                   if name.startswith(prefix)]
        for member in members:
            for pattern, label in FORBIDDEN_PATTERNS:
                if re.search(pattern, member.lower()):
                    problems.append(f"contains a {label}: {member}")
                    break
        present = set(members)
        required = REQUIRED_FILES + (f"docs/releases/{tag_for(version)}.md",)
        for name in required:
            if name not in present:
                problems.append(f"is missing {name}")
        if "src/__init__.py" in present:
            packaged = _VERSION.findall(
                bundle.read(prefix + "src/__init__.py").decode("utf-8"))
            if packaged != [version]:
                problems.append(
                    f"carries version {packaged}, expected {version}")
    if problems:
        raise BuildRefused("the package " + "; ".join(problems))
    return members


def sha256_of(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def build(repo=REPO_ROOT, output_dir=None, expect_tag=None):
    """Build and verify the package. Returns a dict describing it."""
    repo = os.path.abspath(repo)
    version = read_version(repo)
    tag = tag_for(version)
    if expect_tag is not None and expect_tag != tag:
        raise BuildRefused(
            f"the tag is {expect_tag!r} but src/__init__.py says version "
            f"{version!r}, whose tag is {tag!r}. Fix one of them; nothing "
            "was built.")
    notes = os.path.join(repo, "docs", "releases", tag + ".md")
    if not os.path.isfile(notes):
        raise BuildRefused(f"release notes are missing: docs/releases/{tag}.md")
    require_clean_tree(repo)
    gate = run_privacy_gate(repo)
    commit = _git(repo, "rev-parse", "HEAD").strip()

    output_dir = os.path.abspath(output_dir or os.path.join(repo, "dist"))
    os.makedirs(output_dir, exist_ok=True)
    zip_name = f"{PACKAGE_NAME}-{version}.zip"
    zip_path = os.path.join(output_dir, zip_name)
    sums_path = os.path.join(output_dir, CHECKSUM_FILE)
    for stale in (zip_path, sums_path):
        if os.path.exists(stale):
            os.remove(stale)
    try:
        # core.autocrlf=false: the package holds the committed bytes, so the
        # same commit gives the same zip on Windows and on a CI runner.
        _git(repo, "-c", "core.autocrlf=false", "archive", "--format=zip",
             f"--prefix={PACKAGE_NAME}-{version}/", "-o", zip_path, commit)
        members = inspect_package(zip_path, version)
    except BaseException:
        if os.path.exists(zip_path):
            os.remove(zip_path)
        raise
    digest = sha256_of(zip_path)
    with open(sums_path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(f"{digest}  {zip_name}\n")
    return {
        "version": version, "tag": tag, "commit": commit,
        "prerelease": is_prerelease(version), "gate": gate,
        "zip": zip_path, "zip_name": zip_name,
        "size_bytes": os.path.getsize(zip_path), "file_count": len(members),
        "sha256": digest, "checksums": sums_path, "notes": notes,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Build dist/tape-ledger-<version>.zip and "
                    "SHA256SUMS.txt from the committed tree. Builds files "
                    "only: no tag, no push, no publishing.")
    parser.add_argument("--expect-tag",
                        help="Refuse unless the version in src/__init__.py "
                             "corresponds to this tag (e.g. v1.0.0-rc.1).")
    parser.add_argument("--output-dir", help="Default: dist/ in the repo.")
    parser.add_argument("--repo", default=REPO_ROOT, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        result = build(args.repo, args.output_dir, args.expect_tag)
    except BuildRefused as exc:
        print(f"[RELEASE] REFUSED: {exc}", file=sys.stderr)
        return 1
    print(f"[RELEASE] version     : {result['version']}  (tag "
          f"{result['tag']}, "
          f"{'pre-release' if result['prerelease'] else 'release'})")
    print(f"[RELEASE] commit      : {result['commit']}")
    print(f"[RELEASE] privacy gate: {result['gate']}")
    print(f"[RELEASE] package     : {result['zip']}")
    print(f"[RELEASE] size        : {result['size_bytes']} bytes, "
          f"{result['file_count']} files")
    print(f"[RELEASE] sha256      : {result['sha256']}")
    print(f"[RELEASE] checksums   : {result['checksums']}")
    print(f"[RELEASE] notes       : {result['notes']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
