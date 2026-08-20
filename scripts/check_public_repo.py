"""Pre-push privacy gate for this PUBLIC repository.

Fails loudly (exit 1) if anything tracked by git looks like operational or
private data: databases/dumps, real manifests, logs, secrets, generated
inventory snapshots, or known private infrastructure identities. Run it
before every push:

    python scripts/check_public_repo.py

The gate scans only what git actually tracks (``git ls-files``), so local
private data (private/, db_backups/, config.ini, .env, manifest roots) does
not trigger it — and must never become tracked.
"""
import re
import subprocess
import sys
import os

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# File names/paths that must never be tracked in a public repo.
FORBIDDEN_PATH_PATTERNS = [
    r"(^|/)\.env$", r"(^|/)\.env\.(?!example$)",
    r"\.(db|sqlite3?|dump|bak|backup)$",
    r"\.restore_list\.txt$", r"\.receipt\.json$",
    r"(^|/)db_backups/", r"(^|/)private/",
    r"(^|/)backup_logs/.+",            # runtime logs and diagnostics
    r"(^|/)storage_map/logs/",
    r"(^|/)storage_map/coverage_baseline\.json$",
    r"(^|/)deliverables/",
    r"storage_map_\d{8}_\d{6}\.html$",  # generated dashboard snapshots
    r"\.(rawlog|log)$",
    r"(^|/)(OWC_SUPPORT_LOGS|.*drive_dump).*\.zip$",
    r"(^|/)config\.ini$",              # the operator config is local-only
    r"\.(dmp|blz)$",                   # drive dumps
    r"(^|/)lto_archive.*\.(tar\.gz|tgz)$",
]

# Content patterns that indicate a credential or private identity leaked into
# a tracked text file. Placeholders used by templates are explicitly allowed.
SECRET_PATTERNS = [
    (re.compile(r"BEGIN (RSA|EC|OPENSSH|PGP)? ?PRIVATE KEY"), "private key"),
    # A credential VALUE (quoted-or-not) that contains a digit — plain
    # identifier plumbing like ``password=password`` never matches.
    (re.compile(r"(?i)\b(password|passwd|secret|api_key|apikey|token)\s*[:=]\s*"
                r"(?!\s*$)(?!change_me)(?!<)(?!\$\{)(?!%\()(?!\*{3})"
                r"['\"]?(?=[A-Za-z0-9+/_\-]*\d)[A-Za-z0-9+/_\-]{8,}"),
     "credential-like assignment"),
    (re.compile(r"postgresql://[^\s'\"<>]*:[^\s'\"<>@*]{4,}@"), "DSN with inline password"),
]

# Synthetic fixtures that legitimately look like secrets (reviewed).
SECRET_ALLOWLIST = {"tests/test_operational_hardening.py"}

# Private infrastructure identities that were scrubbed from the public tree.
# The literals are built by concatenation so this file never matches its own
# patterns (and survives mechanical scrub passes).
PRIVATE_IDENTITY_PATTERNS = [
    (re.compile(r"\bso0[12]\b"), "internal source hostname"),
    (re.compile(r"LAB-" + r"HPLB-09", re.I), "production host name"),
    (re.compile(r"10970" + r"08774"), "drive serial"),
    (re.compile(r"Tech" + r"nion", re.I), "organization name"),
    (re.compile(r"/st" + r"rg/[CDE]/"), "real remote data path"),
]

TEXT_EXTENSIONS = {".py", ".md", ".txt", ".ini", ".sql", ".yml", ".yaml",
                   ".json", ".ps1", ".cfg", ".toml", ".example", ".html"}

# Files allowed to mention the placeholder-policy patterns for documentation
# (none today; add repo-relative paths deliberately if ever needed).
IDENTITY_ALLOWLIST: set = set()


def tracked_files():
    out = subprocess.run(["git", "ls-files"], cwd=REPO, capture_output=True,
                         text=True, check=True)
    return [line.strip() for line in out.stdout.splitlines() if line.strip()]


def main():
    failures = []
    files = tracked_files()

    for path in files:
        for pattern in FORBIDDEN_PATH_PATTERNS:
            if re.search(pattern, path):
                failures.append(f"FORBIDDEN TRACKED PATH: {path}  (rule: {pattern})")
                break

    for path in files:
        ext = os.path.splitext(path)[1].lower()
        if ext not in TEXT_EXTENSIONS:
            continue
        full = os.path.join(REPO, path)
        try:
            with open(full, encoding="utf-8", errors="ignore") as handle:
                content = handle.read()
        except OSError:
            continue
        if path not in SECRET_ALLOWLIST:
            for pattern, label in SECRET_PATTERNS:
                match = pattern.search(content)
                if match:
                    snippet = match.group(0)[:12]
                    failures.append(f"SECRET? {label}: {path} ({snippet!r}...)")
        if path in IDENTITY_ALLOWLIST:
            continue
        for pattern, label in PRIVATE_IDENTITY_PATTERNS:
            if pattern.search(content):
                failures.append(f"PRIVATE IDENTITY ({label}): {path}")

    if failures:
        print("PUBLIC-REPO GATE FAILED:")
        for failure in sorted(set(failures)):
            print("  -", failure)
        print(f"\n{len(set(failures))} finding(s). Fix (untrack/redact) before pushing.")
        return 1
    print(f"PUBLIC-REPO GATE PASSED: {len(files)} tracked files clean.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
