# Third-party licences

OWC LTO-8 Archiver itself is released under the [MIT License](LICENSE),
copyright (c) 2026 Raz Ben Aharon.

This repository and its release package contain **no third-party code**. The
Python packages below are installed by `pip` from the Python Package Index
when you install a requirements file; they are not copied into this
repository or redistributed with it.

The licence of each package was read from the metadata of the installed
distribution (`importlib.metadata`) on 2026-11-16, for the versions shown.
The requirements files set minimum versions only, so `pip` may install newer
ones; check the metadata of what you install.

## Python packages

| Package | Version checked | Licence (from package metadata) | Installed by | Needed for |
| --- | --- | --- | --- | --- |
| psutil | 7.2.2 | BSD-3-Clause | `requirements-core.txt` | Process priority and CPU affinity for the tape writer; memory figures. Optional at run time |
| zstandard | 0.25.0 | BSD-3-Clause | `requirements-core.txt` | Compressed manifests (`*.jsonl.zst`) |
| psycopg | 3.3.4 | LGPL-3.0-only | `requirements-postgres.txt` | PostgreSQL backend only |
| psycopg-binary | 3.3.4 | LGPL-3.0-only | `requirements-postgres.txt` (`psycopg[binary]`) | PostgreSQL backend only |
| psycopg-pool | 3.3.1 | LGPL-3.0-only | `requirements-postgres.txt` (`psycopg[pool]`) | PostgreSQL backend only |
| typing_extensions | 4.15.0 | PSF-2.0 | dependency of psycopg-pool | PostgreSQL backend only |
| tzdata | 2026.2 | Apache-2.0 | dependency of psycopg on Windows | PostgreSQL backend only |
| PySide6, PySide6_Essentials, PySide6_Addons, shiboken6 | 6.11.1 | LGPL-3.0-only OR GPL-2.0-only OR GPL-3.0-only | `requirements-gui.txt` | Inspector GUI only |
| pytest | 9.1.1 | MIT | installed separately, for development | Running the tests |

## Compatibility with the MIT licence of this project

- **psutil, zstandard, typing_extensions, tzdata, pytest** have permissive
  licences (BSD, PSF, Apache-2.0, MIT). Using them from MIT-licensed code
  raises no conflict.
- **psycopg** (LGPL-3.0-only) is an optional dependency. This project imports
  it at run time, unmodified, only when the PostgreSQL backend is selected;
  it does not copy, modify or bundle it. A SQLite-only installation does not
  install it.
- **PySide6 / Qt for Python** (LGPL-3.0-only or GPL) is an optional
  dependency for the inspector GUI only. It is imported at run time,
  unmodified, and is neither bundled nor redistributed here.

If you build and distribute a package that **includes** psycopg or PySide6
(for example a frozen executable), the LGPL obligations for those libraries
apply to your distribution. This is a statement of facts about the
dependencies, not legal advice.

## Software that is not included

The following is required for tape work, is proprietary or separately
licensed, and is **not included in or redistributed with** this project.
Obtain it from its vendor under the vendor's terms:

- IBM LTFS (IBM Spectrum Archive / Storage Archive Single Drive Edition) and
  its command-line tools (`LtfsCmdFormat`, `LtfsCmdCheck`, `LtfsCmdDrives`,
  `LtfsCmdEject`);
- tape-drive firmware, host-bus-adapter drivers and vendor diagnostic tools;
- Microsoft Windows and its tools (`robocopy`, `wmic`, OpenSSH client);
- PostgreSQL and Docker, when the PostgreSQL backend is used. The bundled
  `docker-compose.yml` only names the public `postgres:17` image; it does not
  contain it.

Product names are trademarks of their owners and are used here only to
identify the software this project works with.
