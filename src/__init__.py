"""Internal package for the LTO archiver."""

#: The single source of the program version (PEP 440). The release tag is the
#: same version spelled the SemVer way: ``1.0.0rc1`` <-> ``v1.0.0-rc.1``
#: (``scripts/build_release.py`` derives one from the other and refuses a
#: mismatch). ``python run.py --version`` and ``python inspect_db.py
#: --version`` print it without opening a database or reading config.ini.
__version__ = "1.0.0rc1"
