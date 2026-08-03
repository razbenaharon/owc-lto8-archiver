# Plan 1 — module boundary and dead-path audit (Task 1.6)

The Task 1.6 acceptance gate asks for two things that are easy to assert and
easy to fudge: that **every retained scanner/planner has a production caller or
a documented compatibility purpose**, and that a **before/after report** shows
`remote_orchestrator.py` materially reduced with no duplicate scheduling
implementation and no new wrapper-only module. This file is that report.

## 1. Symbol audit

Repository-wide search over `*.py`, `*.ini` and `*.md` (excluding
`__pycache__` and the plan documents themselves).

| Symbol | Production caller | Verdict |
|---|---|---|
| `scanning.StreamingRemoteScanner` | **No production caller** | Retained only for compatibility characterization. The persistent frontier is the sole production scanner; this class is not a legacy scan mode. |
| `scanning.RemoteScanner` | **No production scanner caller** | Retained for parsing compatibility tests. Its `scan()`/`_scan_one()` batch entry has no production caller but is exercised by `RemoteScannerTests`, which proves the historical find-warning and truncated-record parsing rules. |
| `planning.ChunkPlanner` | **Yes** — `StreamingChunkBuilder` composes it for `footprint()` | Retained. |
| `planning.StreamingChunkBuilder` | **Yes** — the production frontier publisher receives it from `RemoteOrchestrator` and calls `builder.add()` only for reconciled survivors | Retained. |
| `scanning.DirectoryFirstRemoteScanner` | **No** | **Removed.** |
| `planning.DirectoryUnitPlanner` | **No** | **Removed.** |
| `planning.DirectoryPlanUnit` | **No** (only `DirectoryFirstRemoteScanner.stat_directory` constructed it) | **Removed.** |
| `config.remote_scan_mode` | **No reader anywhere** | **Removed**, with its `config.example.ini` key. |
| `config.remote_scan_depth` | **No reader** (only `DirectoryFirstRemoteScanner`'s `depth`) | **Removed.** |
| `config.directory_chunk_max_gb` | **No reader** | **Removed.** |
| `config.directory_chunk_max_files` | **No reader** | **Removed.** |
| `config.large_file_min_mb` | **No reader** today | **Retained** — it is a real archival policy figure (loose vs packed), defaults from `index_min_file_mb`, and Plans 2–3 size stored-TAR/loose behaviour on it. Documented as currently unread. |
| `config.incremental_scan` | **No runtime mode exists** | **Removed** as a feature property/example key. An old live key is deprecated and ignored; it cannot change scanner behaviour. |

An existing `config.ini` may still contain removed directory-first keys, which
`configparser` ignores. The obsolete `incremental_scan` key is also ignored but
emits one deprecation warning per process; remove that line from the live config.

### Why the directory-first code was not revived instead

Plan 1 warns: *"Do not reuse the dormant directory-first code without proving it
avoids recursive duplicate walks and uses the real loose threshold."* It fails
both:

1. **Recursive duplicate walks.** `stat_directory()` ran
   `find "$root" -type f -printf '%s %h\0'` — a *full recursive* listing — once
   per candidate directory. `DirectoryUnitPlanner._append()` then descends into
   children when a unit overflows, so planning a tree of depth *d* re-walked
   every file up to *d* times. On the source this pipeline actually archives
   (~82 M files still to go at the time of writing) that is not a tuning
   problem, it is a different order of magnitude.
2. **Two different thresholds from one setting.** `stat_directory()` computes
   `threshold = int(large_file_min_mb * 1024 * 1024)` — **bytes** — while
   `iter_large_files()` computes `threshold = int(large_file_min_mb)` and passes
   it to `find -size +{threshold-1}M` — **megabytes**. The same configured value
   therefore meant two different sizes in the same class, so its "large file"
   accounting and its large-file listing could never agree.

Task 2.3 builds the directory-boundary scanner from scratch, on an
immediate-child listing with persisted per-directory state, for exactly these
reasons.

## 2. Before / after module report

Counted with `ast`: `lines` is physical lines, `defs` is function/method
definitions, `branch` counts `if`/`for`/`while`/`try`/`except`/`with` nodes as a
proxy for control-flow density.

| module | lines | defs | branches |
|---|---:|---:|---:|
| `remote_orchestrator.py` **before Plan 1** | 3657 | 78 | 378 |
| `remote_orchestrator.py` after | 2506 | 77 | 163 |
| `scan_frontier.py` | 1111 | 42 | 123 |
| `remote_staging.py` | 1137 | 21 | 128 |
| `remote_writer.py` | 351 | 6 | 30 |
| `remote_pipeline.py` | 407 | 16 | 40 |
| `scanning.py` | 543 | 20 | 74 |
| `planning.py` | 86 | 6 | 8 |

**Control flow, not just line count, is what moved.** `remote_orchestrator.py`
lost **57% of its branches** (378 → 163) while materially reducing its lines: what
left is decision-making, not boilerplate. Its remaining `def` count barely
changed because Task 1.2 deliberately kept the façade's public API — the
delegating one-liners are the API surface, not the behaviour.

The five-module total exceeds the original 3657 because each new module
carries its own imports and a docstring explaining the invariant it protects.
That is the intended trade: the number that matters operationally is that the
single largest file dropped from 3657 to 2506,
and that the finite tape-write group is now 351 reviewable lines in one place.
Cartridge checks remain in `remote_orchestrator.py`, so those 351 lines are the
sole write-group entry path, not the whole cartridge-access surface.

## 3. No duplicate scheduling implementation

- **One production scan/publication implementation**:
  `scan_frontier.FrontierScanCoordinator`.
  `remote_orchestrator.py` constructs the builder factory but contains no
  `iter_scan`, `_append_chunk`, or `_scanner_planner`; the frontier coordinator
  owns traversal and publication.
- **One scheduling loop**: `remote_pipeline.RemotePipelineCoordinator`, used by
  *both* `_run_streaming_session` and `_run_session`. The scan-complete resume
  path's group-of-one bypass is gone
  (`tests/test_remote_failure_hardening.py::SingleGateStructureTests`).
- **One finite tape-write path**: `remote_writer.RemoteChunkWriter._write_chunk_group`, which
  is the only caller of `_pre_write_safety_gate`
  (`tests/test_finite_group_only_tape_path.py`).

## 4. No wrapper-only module

Each new module owns behaviour rather than forwarding:

| module | owns |
|---|---|
| `scan_frontier` | the sole production frontier, schema readiness, discovery, chunk sealing/publication, and the publication gate |
| `remote_staging` | SSH/tar fetch, retry classification, the staging watchdog, packing, preserve/discard/resume-pack rules |
| `remote_writer` | ownership acquisition, the single safety gate, per-chunk failure isolation, the `backing` transition |
| `remote_pipeline` | authoritative work selection, backlog fairness, producer/writer lifecycle, group settlement |

`RemoteOrchestrator` retains delegating methods, but they are the *façade's*
public API — the surface callers and the existing test suite address — not a
second implementation. The behaviour they call into exists once.
