# Tape and archive state

**Snapshot: operator workstation, as observed 2026-08-20; campaign outcome
and manifest location updated 2026-09-18/23.** This is the single
place tape/session/catalog state is recorded; every other document links here
instead of restating counts. A dated document is a snapshot — verify live
state (PostgreSQL, receipts, LTFS logs) before acting.

## Tapes

| Tape | Status | Contents | Catalog evidence |
| --- | --- | --- | --- |
| **Tape_01** | **CLOSED production — immutable** | 4,255,539 files, 10,624,686,466,311 bytes (10.62 TB): 4,254,947 packed small files in 295 ZIP bundles + 592 loose files | Fully inventoried: per-file manifests (export 1, pruned) + `files_index` loose/large rows + folder aggregates |
| **Tape_02** | **CLOSED production — immutable, read-only** (PWE latch, [incident 010](incidents/010-20260724-ltfs-write-perm-readonly.md)) | Pre-July: 12,965 files, 3,203,839,476,694 bytes (3.20 TB) in 20 bundles. Additionally: remote Session 37 chunks 0–48 (~710 GB, written 2026-07-09..24) | Pre-July content fully inventoried locally (as Tape_01). The Session 37 chunk 0–48 inventory lives in the **production-host catalog and its plan manifests**, which are not on this workstation — see "Known gaps" |
| **Tape_03** | NOT production — scratch | Generation 3 carries the 24 GiB Phase 5E synthetic pilot + an **unverified, uncataloged** partial campaign copy (chunks 49–81, [incident 014](incidents/014-20260819-campaign-write-servo-halt.md)). Generations 1–2 retired | Zero `archive_runs`, zero `files_index` rows, `used_space = 0` — by design |
| **Tape_04** | NOT production — scratch | Generation 1: an **unverified, uncataloged** partial campaign copy (chunks 49–81/82); 68-file LTFS index persisted at the servo fault | No catalog rows anywhere |

**Production boundary: Tape_01 and Tape_02 are the only closed production
tapes. Nothing may write to, reformat, or reinterpret them, and their catalog
records and manifests must never be deleted or reset.** Tape_03/Tape_04
content is scratch until a healthy drive verifies or rewrites it.

## Drive

The LTO-8 drive failed with a recurring Track Following (servo) Permanent
Write Error across four cartridges (incidents
[010](incidents/010-20260724-ltfs-write-perm-readonly.md),
[013](incidents/013-20260809-tape03-servo-drive-failure.md),
[014](incidents/014-20260819-campaign-write-servo-halt.md)) and finally could
not thread a cartridge at all. Replacement/RMA is in progress. **No tape
operation is possible or permitted until a replacement drive passes a
synthetic pilot on a scratch cartridge.**

## Sessions

| Session | Kind | State |
| --- | --- | --- |
| 9, 10, 11 | local | `completed`; their output is the Tape_01/Tape_02 closed content |
| 34, 35, 36 | remote | Historical (production-host catalog): 34 completed, 35 abandoned-before-start, 36 superseded by 37 except its unique chunk 0 on Tape_02 |
| 37 | remote | Chunks 0–48 `done` on Tape_02 (closed). Chunks 49–216 **localized** as stored-TAR containers with verified receipts (2026-08-10..12); waiting for a healthy drive. Chunks 108/109 abandoned fetch state was exported to an `abandoned-fetch-state-v1` artifact and cleared (2026-08-05). Chunk 217: container without receipt — must be re-localized |

There is **no live/abandoned session state in the local catalog**: no
`active` session rows, no stale locks, no in-flight chunks. The next run
starts clean.

## The localization campaign store

`LTO_METADATA/LOCAL_MANIFEST_ARCHIVE/campaign_tape03/` on the external
campaign drive held one directory per chunk (49–217), each with
`container_0000.tar` (+ more ordinals where the chunk exceeded one container)
and `receipt.json` carrying container SHA-256, sidecar SHA-256, member count,
logical bytes, and plan-manifest locators. Receipts exist for chunks 49–216;
chunk 217 has none.

Measured by `scripts/verify_campaign_store.py --mode structure`, 2026-08-20:
**168 chunks / 184 containers / 695.3 GiB**, every container present at its
receipted size, zero truncation, zero I/O errors. Content (SHA-256)
verification has **not** completed — see below.

> **OUTCOME ([incident 015](incidents/015-20260820-campaign-drive-instability.md),
> 2026-08-21): the campaign drive failed during evacuation and no longer
> enumerates. Rescued: 147 of 168 receipts (198.3 KB) and 0 of 184
> containers — 695.3 GiB of staged payload is gone.** This was a staging
> copy, never the backup: Tape_01/Tape_02 and their manifests were not
> affected.
>
> **2026-09-23: Session 37 abandoned by operator decision.** Nothing is
> re-fetched. Chunks 0–48 stay on Tape_02 as written; Tape_03/Tape_04 are
> plain scratch to reformat. The local catalog holds zero Session 37 rows,
> and Known gaps 1–2 below are accepted, not pending.

## Catalogs and manifests

- **Local PostgreSQL catalog** (`lto_archive`, Docker `lto_pg`): the
  authoritative catalog on this workstation. Schema at migrations 001–019.
  After the 2026-08-20 manifest export + prune it holds **no per-small-file
  inventory**: packed small files live in the per-file JSONL.zst manifests;
  the DB keeps the export ledger, folder aggregates, loose/large file rows,
  sessions/chunks/bundles/runs, and tape state.
- **Per-file manifests**:
  `E:\Projects\owc-lto8-archiver\LTO_METADATA\LOCAL_MANIFEST_ARCHIVE\` —
  on the internal fixed ReFS Dev Drive, not on the unstable external drive.
  Relocated here 2026-09-18 from the operator's Desktop, where a full-disk
  sweep found it was the **only** copy on the workstation; the tree is
  git-ignored (`LTO_METADATA/`). 145 segments, 241 MiB compressed,
  inventorying 1.34 TiB of source files.
  Layout: `<Tape_label>/<session>/bundle_<id>.jsonl.zst`, each segment
  SHA-256-recorded in `local_manifest_segments`. Verified after the move by
  `scripts/validate_archive_reconciliation.py --heavy`: disk == ledger ==
  aggregates at 4,240,566 rows / 88,770 directories, all six checks PASS.
- **Production-host catalog** (directory-catalog database, last authoritative
  2026-08-03..05): contains the full Session 36/37 planning rows (23.2M plan
  members) and the plan manifests / tar sidecars referenced by the campaign
  receipts. It is on the (currently unreachable) production host — see
  "Known gaps".

## Known gaps (recorded, not hidden)

1. **Session 37 chunks 0–48 per-file inventory is not on this workstation.**
   Tape_02's closed content from July 2026 is represented locally only at
   the coarse level. The authoritative inventory (production catalog +
   plan manifests + ZIP bundle metadata) lives on the production host's
   metadata root. Before that host or its disk is retired, its
   `LTO_METADATA` tree and a fresh catalog dump must be copied here.
2. **Campaign receipts reference sidecar/plan-manifest locators**
   (`tar_sidecars/...`, `plan_manifests/...`) that resolve on the production
   host's metadata root, not on this workstation. The containers themselves
   are self-describing (a stored TAR carries its member inventory), so
   restore is possible without them, but the referenced artifacts should be
   recovered with the production metadata root.
3. Tape_03/Tape_04 hold partial unverified campaign copies that no catalog
   references; when a healthy drive exists, decide per tape: verify & adopt,
   or rewrite from the (evacuated) campaign store.
