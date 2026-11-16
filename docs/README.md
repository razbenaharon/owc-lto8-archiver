# Documentation map

| You want to | Read |
| --- | --- |
| Know what this is and try it | [../README.md](../README.md) |
| Install it, or upgrade an existing PostgreSQL installation | [installation.md](installation.md) |
| Look up a `config.ini` key | [configuration.md](configuration.md) |
| Archive, restore, maintain tapes, back up the catalog, stop a run | [user-guide.md](user-guide.md) |
| Know the rules that protect the tape | [safety.md](safety.md) |
| Understand an error message or an exit code | [troubleshooting.md](troubleshooting.md) |
| Choose between SQLite and PostgreSQL; see what is PostgreSQL-only | [database-backends.md](database-backends.md) |
| Understand modules, locks, the chunk state machine, the safety gates | [architecture.md](architecture.md) |
| Run the tests; see what is and is not verified; plan a hardware pilot | [testing-and-validation.md](testing-and-validation.md) |
| See what changed | [../CHANGELOG.md](../CHANGELOG.md), [releases/v1.0.0-rc.1.md](releases/v1.0.0-rc.1.md) |
| Contribute | [../CONTRIBUTING.md](../CONTRIBUTING.md), [../AGENTS.md](../AGENTS.md) |
| See the manifest and receipt formats | [../examples/README.md](../examples/README.md) |
| Check licences | [../LICENSE](../LICENSE), [../THIRD_PARTY_LICENSES.md](../THIRD_PARTY_LICENSES.md) |
| Read how the rules were learned | [history/README.md](history/README.md) |

## Which source wins

When two sources disagree:

| Question | Authoritative source |
| --- | --- |
| What the program does | The code and its tests |
| Which commands and flags exist | The parsers in `run.py` and `inspect_db.py`. `python inspect_db.py --help` lists the inspector's; `run.py` has no top-level help, so use [user-guide.md](user-guide.md), which a test checks against the real parsers |
| Which settings exist | `CONFIG_SCHEMA` in `src/config.py`; [configuration.md](configuration.md) is tested against it |
| What is safe at the tape | [safety.md](safety.md). If a rule and a convenience conflict, the rule wins |
| Session and chunk state | The catalog database, not a document |
| The LTFS sync mode in force | The mount itself (LTFS event 61259), not `ltfs.conf.local` |
| Drive and medium health | The IBM LTFS log for the current mount |
| What happened at the original deployment | [history/](history/README.md): dated records, not instructions |

## Keeping the documentation true

- A command or flag written in `README.md`, `CONTRIBUTING.md`, `AGENTS.md`
  or `docs/*.md` must be accepted by the program. `tests/test_docs_commands.py` fails
  otherwise. Run a new command before documenting it.
- A relative link must resolve. The same test checks every Markdown file.
- A new `config.ini` key needs a row in [configuration.md](configuration.md).
- Do not write counts that change (tests, lines, files) without a date.
- Do not add site-specific state (host names, tape labels, session numbers)
  to the guides. A dated record belongs under [history/](history/README.md).
- Use placeholders: `source-host.example`, user `archive`, `D:\staging`.
