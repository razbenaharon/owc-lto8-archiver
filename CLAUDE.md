# CLAUDE.md

AI-assistant guidance for this repository lives in **[AGENTS.md](AGENTS.md)** — read it
first. It covers project structure, build/run commands, coding style, testing,
logging/reports, performance characteristics, and security/operations.

This file is intentionally thin so the guidance has a single source of truth.

## Do not miss

> **During archive writes, avoid browsing the LTFS drive or starting separate copy
> jobs.** Internal tape access is serialized (`_acquire_tape_io_lock`), but external
> processes can still degrade tape throughput. This warning is also printed at the
> start of every tape-write run (`LTFS_WRITE_WARNING`).
