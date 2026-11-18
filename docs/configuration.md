# Configuration reference

All settings live in `config.ini` in the project folder. Create it by copying
the template:

```powershell
Copy-Item config.example.ini config.ini
```

If `config.ini` does not exist, the first start writes one with default
values and the SQLite backend. `config.ini` and `.env` are ignored by git.

This page lists every key of `CONFIG_SCHEMA` in `src/config.py`. A test
(`tests/test_docs_commands.py`) fails when a key is added to the schema and
not to this page, or when a type here differs from the schema.

## Rules of the file

The file is checked when the program starts. A bad value stops the start with
a message that names the section, the key and the value.

- **No inline comments.** `guard = true ; keep on` is an error. `;` and `#`
  are legal inside values, so nothing after a value is stripped. Put each
  comment on its own line.
- Yes/no values: `true false yes no on off 1 0`, any case.
- Numbers must be finite and inside the range in the Type column.
- Section names are case-sensitive and upper case.
- An unknown section or key is reported as a warning and ignored. The warning
  suggests the nearest known name, so read the warnings after an edit.
- Paths may be quoted. Do not put a trailing comment after a path.

Type column: `>= 1` and `> 0` are the limits the startup check enforces.
*(inert)* means the key is accepted and checked but nothing reads it.
`retired` means the key is accepted only so that an old file still loads.

Default column: the value used when the key is absent. `config.example.ini`
ships different values for some keys (for example `zip_threshold_mb = 2048`);
they are examples from one installation, not recommendations.

## Secrets: `.env`

Passwords do not belong in `config.ini`. Copy `.env.example` to `.env` in the
project folder and set the values there. Order of precedence for each secret:
process environment variable, then `.env`, then `config.ini`.

| Variable | Used for |
| --- | --- |
| `PGPASSWORD` | PostgreSQL password. Also read by `docker-compose.yml`. Not needed with SQLite. |
| `REMOTE_PASSWORD` | SSH password of the source host. Leave empty to use key-based login. |
| `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` | Notifications (retired in this version). |
| `LTO_SSH_HOSTKEY_POLICY` | SSH host-key policy for all remote operations: `accept-new` (default, trust on first contact) or `yes` (strict, after you have filled `known_hosts`). |

Two more environment variables are read by the runners: `LTO_DEBUG=1` prints
the full traceback of an operator-facing error on the console, and
`LTO_TEST_PG_DSN` selects the disposable PostgreSQL server for the tests (see
[testing-and-validation.md](testing-and-validation.md)).

## Minimal examples

SQLite, no tape drive attached (enough for a first look):

```ini
[PATHS]
source_dir = D:\staging\source
staging_dir = D:\staging\work
restore_dir = D:\staging\restored

[DATABASE]
backend = sqlite
sqlite_path = lto_archive.sqlite3

[HARDWARE]
lto_drive =
```

PostgreSQL with a tape drive and a remote source:

```ini
[DATABASE]
backend = postgresql
host = localhost
port = 5432
dbname = lto_archive
user = lto
sslmode = prefer

[HARDWARE]
lto_drive = T:\\
ltfs_ownership_id = drive_0000000000
eject_after_session = false

[REMOTE]
remote_host = source-host.example
remote_user = archive
remote_path = /data/projects
tape_label = Tape_01
```


## `[PATHS]`

Folders the program reads and writes. Use absolute paths; a relative path is
resolved against the project folder. Quotes around a path are accepted. The
placeholder paths in `config.example.ini` are used literally if you leave
them: replace all of them before the first start.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `source_dir` | str | `source` in the project folder | Folder the local archive (menu 1) scans. |
| `staging_dir` | str | `staging` in the project folder | Local working area: fetched and packed chunks wait here before the tape write, and restores stage bundles here. Put it on a fast local disk with room for several chunks. |
| `restore_dir` | str | `restored` in the project folder | Where restored files are written. A restore never writes outside this folder. |
| `backup_log_dir` | str | `backup_logs` in the project folder | Where `SUMMARY.csv`, `archiver.log` and the tape-write logs go. The folder is created if missing. |

## `[DATABASE]`

Which catalog database to use. See [database-backends.md](database-backends.md).

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `backend` | choice: sqlite / postgresql | `postgresql` | Catalog backend (`postgres` is accepted as another spelling of `postgresql`). **Absent means PostgreSQL**, so an install that predates SQLite is never switched. `config.example.ini` and the config generated on first run set `sqlite`. |
| `sqlite_path` | str | `lto_archive.sqlite3` | SQLite catalog file. A bare name lives in the project folder. Must be on a local disk; a network path is refused. |
| `host` | str | `localhost` | PostgreSQL server host. |
| `port` | int, >= 1, <= 65535 | `5432` | PostgreSQL server port. |
| `dbname` | str | `lto_archive` | PostgreSQL database name. |
| `user` | str | `lto` | PostgreSQL user. |
| `password` | str | empty | PostgreSQL password. **Do not set it here** - set `PGPASSWORD` in the environment or in `.env`, which take precedence. Read from this file only so an old config keeps working. |
| `sslmode` | choice: disable / allow / prefer / require / verify-ca / verify-full | `prefer` | libpq TLS mode. Use `require` or stricter for any server that is not on the loopback interface. |

## `[HARDWARE]`

The tape drive.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `lto_drive` | str | `D:\\` | Drive letter of the LTFS mount, for example `T:\\`. The letter can change between mounts - check it before every run. Leave the value blank when no tape drive is attached: catalog backups then work from any folder. |
| `ibm_eject_cmd` | str | `C:\Program Files\IBM\LTFS\LtfsCmdEject.exe` | Path of the IBM LTFS eject tool. |
| `eject_after_session` | bool | `false` | Eject the cartridge when a local (menu 1) session completes, after the operator types `EJECT`. Keep `false` unless somebody is at the drive. The remote pipeline never ejects, whatever this says. |
| `ltfs_ownership_id` | str | none (required for tape access) | Stable name of the physical drive, for example its serial number. It names the cross-process mutex that lets only one process touch LTFS. 1-64 characters from `A-Z a-z 0-9 . _ -`. If it is missing or malformed, every LTFS operation fails closed. Never derive it from the drive letter. |
| `ltfs_writer_lock_timeout_seconds` | float, > 0 | `300` | How long a tape writer waits for LTFS ownership. Clamped to 1-3600. |
| `ltfs_helper_lock_timeout_seconds` | float, > 0 | `30` | How long a helper or diagnostic waits for LTFS ownership. Clamped to 1-3600. |
| `tape_generation` | choice: lto-6 / lto-7 / lto-m8 / lto-8 / lto-9 | none: the legacy LTO-8 budget | The generation of the cartridges this host writes. It sets the per-cartridge planning budget: native capacity minus a fixed 5% margin, which no setting can remove. LTO-8 = 11.5 TB (the figures the archiver always used), LTO-9 = 17.1 TB, LTO-M8 = 8.6 TB, LTO-7 = 5.7 TB, LTO-6 = 2.4 TB. It also sets the default capacity offered when a cartridge is registered. Name the generation, never a capacity figure: the box prints native and compressed capacity side by side. Case does not matter, and `LTO9`-style spellings without the dash are accepted. Any other value refuses to start. LTO-10 is not listed yet. See [the notes below](#tape_generation). |

### `tape_generation`

- **No line, or `LTO-8`:** the budget is exactly what the archiver always
  planned. Cartridges keep their registered capacity.
- **Older cartridges on a newer host.** With a generation set, a cartridge's
  registered capacity also gets the 5% margin. A cartridge registered with the
  old LTO-8 default (12288 GB) keeps the LTO-8 budget, so an LTO-8 cartridge
  loaded on an LTO-9 host is never planned past what it holds.
- **Registering.** With a generation set, a capacity above that generation's
  native figure is refused when a cartridge is registered or its capacity is
  changed. Such a figure is almost always the compressed one.
- **Changing it.** Change it only between sessions. The budget is checked
  again before every chunk and every write group. A local session resumed
  with chunks larger than the new budget refuses to start and asks you to
  re-plan, before anything touches the drive. Every tape-write run prints the
  budget in force.

## `[SETTINGS]`

Packing.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `zip_threshold_mb` | float, >= 0 | `100` | Files smaller than this are packed into bundles; larger files are copied to tape as they are. |
| `max_zip_size_gb` | float, > 0 | `100` | Largest size of one ZIP bundle. |
| `stored_tar_max_size_gb` | float, > 0 | value of `max_zip_size_gb` | Largest size of one Stored TAR container. |

## `[CATALOG]`

What the catalog records per file.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `index_min_file_mb` | float, >= 0 | `10` | Packed files below this size are listed in per-bundle manifests instead of one catalog row each. Larger files stay searchable as catalog rows. |
| `index_packed_small_files` | bool | `false` | `true` also writes one catalog row per packed small file. Costs catalog size and write time. |
| `index_directory_stats` | bool (inert) | - | No effect: directory statistics are always indexed. |
| `index_full_directory_tree` | bool (inert) | - | No effect: the directory tree is always indexed. |
| `prefer_directory_containers` | bool (inert) | - | No effect: the packer chooses the container. |
| `small_file_manifest_enabled` | bool | `true` | Write the per-bundle manifest of packed small files. |
| `small_file_manifest_format` | choice: jsonl | `jsonl` | Manifest format. |
| `small_file_manifest_compression` | str | `zstd` | Manifest compression. |

## `[REMOTE]`

The remote source host for remote archive (menu 6 and `run.py remote-archive`).

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `remote_host` | str | empty | Source host name, for example `source-host.example`. Required for remote archive. |
| `remote_user` | str | empty | SSH user on the source host. Required for remote archive. |
| `remote_password` | str | empty | SSH password. **Leave blank here** - set `REMOTE_PASSWORD` in the environment or in `.env`, or use key-based login. |
| `remote_path` | str | empty | Root folder on the source host. Required for remote archive. |
| `remote_selected_paths` | str | empty | Folders to archive, one per indented line, each under `remote_path`. Empty means all of `remote_path`. |
| `confirm_before_backup` | bool | `true` | Ask for a typed `yes` before a new session starts writing to tape. |
| `tape_label` | str | empty | Cartridge a **new** session targets, so the target can be named without reading the drive. Blank means the program asks. A resumed session ignores it and keeps its recorded cartridge. |
| `staging_fill_pct` | float, > 0, <= 1 | `0.80` | Fraction of the staging disk a chunk may fill. |
| `large_file_min_mb` | float, >= 0 (inert) | - | No effect: `[CATALOG] index_min_file_mb` is the threshold in force. |
| `incremental_scan` | retired | - | Ignored, with a warning. Remove it. |
| `scan_mode` | retired | - | Ignored, with a warning. Remove it. |
| `remote_scan_depth` | retired | - | Ignored, with a warning. Remove it. |
| `directory_chunk_max_gb` | retired | - | Ignored, with a warning. Remove it. |
| `directory_chunk_max_files` | retired | - | Ignored, with a warning. Remove it. |

## `[PIPELINE]`

The queue of chunks that are staged and ready for the tape writer. Byte values are whole bytes on the staging disk, never memory. Required order: `min_ready_bytes_before_writer_start` <= `target_ready_bytes` <= `max_ready_bytes`; if the order is wrong, or the queue plus its reserve does not fit in `[PERFORMANCE] staging_max_gb`, all four limits fall back together to their defaults, and the run refuses to start if even those do not fit.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `min_ready_bytes_before_writer_start` | int, >= 1 | `21474836480` (20 GiB) | Ready bytes that must accumulate before the writer starts a write group. |
| `target_ready_bytes` | int, >= 1 | `42949672960` (40 GiB) | Ready bytes the pipeline aims to hold. |
| `max_ready_bytes` | int, >= 1 | `85899345920` (80 GiB) | Upper limit of ready bytes. |
| `max_ready_chunks` | int, >= 1 | `48` | Upper limit of ready chunks. |
| `ready_queue_staging_reserve_bytes` | int, >= 0 | `0` | Staging space the ready queue must never use. `0` means automatic: twice `chunk_cap_gb` plus a fixed reserve. |
| `max_unstaged_backlog_chunks` | int, >= 1 | `64` | How many planned chunks may wait for staging before the scanner pauses. |
| `max_write_groups_per_run` | int, >= 0 | `0` | Stop the run after this many completed write groups, always at a chunk boundary. `0` means no limit; `1` writes one bounded group. |
| `write_only_resume` | bool | `false` | `true` writes chunks that are already planned and runs no scanner. Keep `false` for a normal session. |

## `[PERFORMANCE]`

Chunk size, staging and memory limits, and what may run while the tape is being written. The defaults protect the tape write from competing I/O; change one value at a time.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `pipeline_profile` | str (inert) | - | No effect: there is one pipeline. |
| `eject_after_pack` | retired | - | Ignored, with a warning: the mid-pipeline eject was removed and the remote pipeline never ejects. Remove it. |
| `chunk_cap_gb` | float, > 0 | `50` | Largest size of one chunk. A smaller chunk limits what one interrupted write can cost. Applies to chunks planned after the change. |
| `chunk_max_files` | int, >= 1 | `100000` | Largest number of files in one chunk. |
| `prefetch_chunks_ahead` | float, >= 0 | `1` | Chunks fetched and packed ahead of the tape writer. Values below 1 are treated as 1. |
| `staging_max_gb` | float, > 0 | `350` | Total staging space the pipeline may use. |
| `ram_soft_limit_pct` | float, > 0, <= 100 | `70` | Memory use, in percent, above which the pipeline slows new work. |
| `ram_hard_limit_pct` | float, > 0, <= 100 | `85` | Memory use, in percent, above which new work waits. |
| `fetch_target_mbs` | float, >= 0 (inert) | - | No effect: fetch is not rate-limited. |
| `fetch_min_free_ram_gb` | float, >= 0 | `16` | Upper bound of the free memory a fetch waits for. |
| `governor_fetch_target_free_ram_gb` | float, >= 0 | smaller of `fetch_min_free_ram_gb` and `4.0` | Free memory a fetch waits for before starting. |
| `governor_fetch_min_free_floor_gb` | float, >= 0 | `2.5` | Lowest free memory at which a fetch may still start. |
| `governor_fetch_total_ram_cap_pct` | float, > 0, <= 100 | `25` | The fetch free-memory target never exceeds this percentage of total memory. |
| `governor_tape_min_free_ram_gb` | float, >= 0 | `3.0` | Free memory required before a tape write starts. |
| `governor_tape_pause_other_stages` | bool | `true` | Pause the other stages while the tape is being written. |
| `governor_tape_exclusive_heavy_stages` | bool | `true` | Run no other heavy stage together with a tape write. |
| `governor_soft_relax_after_seconds` | float, >= 0 | `120` | Waiting time after which the free-memory target of a waiting stage is lowered one step. `0` never lowers it. |
| `governor_soft_relax_factor` | float, > 0, <= 1 | `0.75` | Factor applied to that target at each step. |
| `governor_status_interval_seconds` | float, >= 0 | `60` | Interval of the governor status line. |
| `governor_memory_sample_interval_seconds` | float, > 0 | `5` | Interval between memory samples. |
| `governor_metadata_batch_size` | int, >= 1 | `10000` | Catalog rows written per batch. |
| `governor_pack_file_batch_size` | int, >= 1 | `10000` | Files handed to the packer per batch. |
| `fetch_parallel_streams` | int, >= 1 | `1` | Concurrent SSH/tar streams per chunk. 2-3 help chunks of many small files. |
| `fetch_transient_retries` | int, >= 0 | `5` | Retries of a fetch after a transient network error (DNS, reset, timeout). `0` fails at once. |
| `fetch_transient_retry_base_seconds` | float, >= 0 | `5` | First retry delay; doubles each attempt, capped at 60 seconds. |
| `fetch_stall_timeout_seconds` | int, >= 0 | `600` | Abort a fetch whose staging folder has not grown for this long; the retry then takes over. `0` never aborts. |
| `pack_parallel_workers` | int, >= 1 | `1` | Concurrent packer threads. |
| `tape_write_exclusive` | bool | `true` | Hold the other stages while the tape is being written, except those an `allow_*_during_tape_write` setting below lets through. `false` holds nothing. |
| `allow_fetch_during_tape_write` | bool | `false` | Let a fetch run during a tape write. |
| `allow_pack_during_tape_write` | bool | `false` | Let packing run during a tape write. |
| `allow_db_sync_during_tape_write` | bool | `false` | Let catalog writes run during a tape write. |
| `allow_db_sync_during_fetch` | bool | `false` | Let catalog writes run during a fetch. |
| `allow_pack_during_fetch` | choice: conditional / true / false | `conditional` | Let packing run during a fetch. `conditional` allows it when memory permits. |
| `allow_pack_above_ram_soft` | bool | `false` | Let packing start above the soft memory limit. |
| `allow_resume_oversized_chunks` | bool | `false` | Let a resumed session continue with chunks larger than the current caps. For a one-time, deliberate resume only. |
| `robocopy_priority` | choice: high / normal / realtime | `high` | CPU priority of the tape-writing `robocopy`. `realtime` needs Administrator. |
| `cpu_affinity` | str | `auto` | CPU cores for fetch and tape work: `auto`, `off`, or an explicit split such as `fetch=0-5;tape=6-7`. |
| `ssh_cipher` | str | `aes128-gcm@openssh.com` | SSH cipher for the fetch stream. |
| `ssh_command_timeout_seconds` | float, > 0 | `3600` | Longest time one remote command may take. |
| `use_mbuffer` | bool | `false` | Pipe the remote `tar` through `mbuffer`, which must be installed on the source host. |
| `mbuffer_size` | str | `512M` | Buffer size passed to `mbuffer`. |
| `staging_padding_factor` | float, > 0 | `1.15` | Planned bytes are multiplied by this when staging space is reserved. Values below 1 are treated as 1. |
| `fetch_overrun_abort_factor` | float, >= 0 | `2.0` | Abort a fetch that exceeds its planned bytes by this factor. `0` only warns. |
| `stored_tar_sidecar_fixed_reserve_bytes` | int, >= 0 | `4096` | Staging bytes reserved for a Stored TAR sidecar, fixed part. |
| `stored_tar_sidecar_member_reserve_bytes` | int, >= 0 | `1024` | Staging bytes reserved for a Stored TAR sidecar, per member. |

## `[TELEGRAM]`

Push notifications. **Retired in this version**: notifications stay off whatever `enabled` says (`TELEGRAM_BOT_RETIRED` in `src/telegram_notify.py`). The keys are still validated.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Turn notifications on (no effect while retired). |
| `timeout_seconds` | float, > 0 | `10` | Timeout of one notification request. |
| `heartbeat_minutes` | float, >= 0 | `30` | Interval of the periodic status message. `0` turns it off. |
| `bot_token` | str | empty | Secret. Set `TELEGRAM_BOT_TOKEN` in `.env`, not here. |
| `chat_id` | str | empty | Secret. Set `TELEGRAM_CHAT_ID` in `.env`, not here. |

## `[LOCAL_MANIFEST_ARCHIVE]`

Permanent local metadata: small-file manifest exports and scan segments. Back it up together with the catalog, and never put it inside `staging_dir`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `root` | str | `LOCAL_MANIFEST_ARCHIVE` in the project folder | Root folder of the local manifest archive. |
| `search_limit` | int, >= 1 (inert) | - | No effect: manifest search takes its limit from the command line (`--limit`). |

## `[WINDOWS_UPDATE]`

Protection against a Windows restart during a tape write.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `guard` | bool | `true` | Pause Windows Update for the run (needs Administrator; a managed host may ignore the pause) and watch for a pending restart. |
| `pause_days` | int, >= 1 | `7` | How long to pause updates. Values above the Windows limit are reduced to it. |
| `block_on_pending_reboot` | bool | `true` | Refuse to start an archive run while a restart is already pending. |

## `[FEATURES]`

Staged features. All default to off and fail closed.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `stored_tar_write_enabled` | bool | `false` | Allow new chunks to be packaged as Stored TAR. Needs PostgreSQL and a packaging boundary; SQLite sessions always use ZIP. Reading and restoring existing Stored TAR containers does not depend on this. |
| `sealed_tape_write_batches_enabled` | bool | `false` | Sealed tape-write batches. PostgreSQL only; needs `scripts/sql/012_sealed_tape_write_batches.sql` applied, otherwise startup refuses. |
| `sealed_tape_write_batches_observation_enabled` | bool | `false` | Log what a sealed batch would have been, without changing the write. PostgreSQL only. |

## `[OBSERVATION]`

Used only when `[FEATURES] sealed_tape_write_batches_observation_enabled` is `true`.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `sealed_batch_observation_queue_max` | int, >= 1 | `100` | Observations that may wait; more are dropped, never the write. |
| `sealed_batch_observation_shutdown_timeout_seconds` | float, >= 0 | `5` | How long shutdown waits for the observation worker. |
| `sealed_batch_observation_statement_timeout_seconds` | float, >= 0 | `5` | Database statement timeout of the observation worker. |
| `sealed_batch_observation_log_path` | str | `lto8_observations\sealed_batch_observations.jsonl` in the system temp folder | Observation log file. Must not be inside the repository, the LTFS mount or the staging folder. |
| `sealed_batch_observation_log_max_bytes` | int, >= 1 | `10485760` | Size at which the observation log rotates. Values below 4096 are treated as 4096. |
| `sealed_batch_observation_log_backups` | int, >= 0 | `5` | Rotated observation logs to keep. |
