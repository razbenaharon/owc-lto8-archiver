"""Regression tests for audit H11: sparse files and hard links in Stored TAR.

Two real-world inputs made a Stored TAR chunk fail on every attempt:

* a sparse file that ends in a hole (GNU tar closes its extent map with a
  zero-length extent at end of file, which the strict reader rejected);
* two planned names that are hard links to one inode (GNU tar writes the
  second as a zero-byte link entry, which the reader rightly rejects).

The reader now accepts exactly the first shape; the producer command asks GNU
tar for regular entries (``--hard-dereference``) so the second cannot occur.
Corruption is still rejected. Where a GNU tar is installed (Git for Windows
ships one) the pinned producer command is run for real on synthetic files.
"""
import io
import os
import shutil
import subprocess
import tarfile
import tempfile
import unittest

from src.pipeline_types import StoredTarExpectedMember
from src.remote_transport import _stored_tar_command
from src.tar_container import StoredTarError, validate_stored_tar
from test_stored_tar import (_gnu_pad, _regular_tar, _sparse_tar, _validate)


def _sparse_map_tar(name, logical_size, extents, stored=b""):
    """A GNU sparse 1.0 member whose map is given as raw (offset, length)."""
    lines = [str(len(extents))]
    for offset, length in extents:
        lines += [str(offset), str(length)]
    map_data = ("\n".join(lines) + "\n").encode("ascii")
    payload = map_data + bytes((-len(map_data)) % 512) + stored
    info = tarfile.TarInfo("GNUSparseFile.1/member")
    info.size = len(payload)
    info.pax_headers = {
        "GNU.sparse.major": "1", "GNU.sparse.minor": "0",
        "GNU.sparse.name": name, "GNU.sparse.realsize": str(logical_size),
    }
    archive = info.tobuf(format=tarfile.PAX_FORMAT) + payload
    archive += bytes((-len(payload)) % 512) + bytes(1024)
    return _gnu_pad(archive)


class TrailingHoleSparseTests(unittest.TestCase):
    def test_file_ending_in_a_hole_is_accepted(self):
        data = _sparse_map_tar(
            "img.bin", 1048576, [(0, 4), (1048576, 0)], stored=b"DATA")
        result = _validate(
            data, [StoredTarExpectedMember("img.bin", 1048576, 0)])
        member = result.members[0]
        self.assertTrue(member.sparse)
        self.assertEqual(member.logical_size, 1048576)
        self.assertEqual(member.sparse_extent_count, 2)

    def test_file_that_is_only_a_hole_is_accepted(self):
        data = _sparse_map_tar("empty.img", 4096, [(4096, 0)])
        result = _validate(
            data, [StoredTarExpectedMember("empty.img", 4096, 0)])
        self.assertEqual(result.members[0].logical_size, 4096)

    def test_zero_length_extent_anywhere_else_is_still_rejected(self):
        cases = {
            "not last": [(0, 0), (10, 4)],
            "before end of file": [(0, 4), (500, 0)],
            "past end of file": [(0, 4), (5000, 0)],
        }
        for label, extents in cases.items():
            with self.subTest(label):
                data = _sparse_map_tar("x", 4096, extents, stored=b"DATA")
                with self.assertRaises(StoredTarError):
                    _validate(data, [StoredTarExpectedMember("x", 4096, 0)])

    def test_terminator_does_not_excuse_missing_or_extra_stored_bytes(self):
        for stored in (b"DAT", b"DATA+"):
            with self.subTest(stored=stored):
                data = _sparse_map_tar(
                    "x", 4096, [(0, 4), (4096, 0)], stored=stored)
                with self.assertRaises(StoredTarError):
                    _validate(data, [StoredTarExpectedMember("x", 4096, 0)])

    def test_trailing_hole_member_with_wrong_planned_size_is_rejected(self):
        data = _sparse_map_tar("x", 4096, [(0, 4), (4096, 0)], stored=b"DATA")
        with self.assertRaisesRegex(StoredTarError, "wrong size"):
            _validate(data, [StoredTarExpectedMember("x", 8192, 0)])

    def test_truncated_and_appended_trailing_hole_archives_are_rejected(self):
        data = _sparse_map_tar("x", 4096, [(0, 4), (4096, 0)], stored=b"DATA")
        plan = [StoredTarExpectedMember("x", 4096, 0)]
        with self.assertRaises(StoredTarError):
            _validate(data[:-512], plan)
        with self.assertRaises(StoredTarError):
            _validate(data + b"x", plan)

    def test_ordinary_sparse_member_is_unchanged(self):
        data = _sparse_tar("s", 100, [(5, b"abc"), (50, b"defg")])
        result = _validate(data, [StoredTarExpectedMember("s", 100, 0)])
        self.assertEqual(result.members[0].sparse_extent_count, 2)


class HardLinkEntryTests(unittest.TestCase):
    def test_producer_dialect_stores_hard_links_as_regular_files(self):
        self.assertIn(" --hard-dereference ", _stored_tar_command("/base"))

    def test_fetch_stream_also_asks_for_regular_entries(self):
        from unittest import mock
        from src import remote_transport
        seen = {}

        def capture(_user, _host, command, **_kwargs):
            seen["command"] = command
            return None, None, "stop here"

        with mock.patch.object(remote_transport, "_ssh_stream_command",
                               side_effect=capture), \
                mock.patch.object(remote_transport, "_has_command",
                                  return_value=True):
            ok, err = remote_transport._remote_tar_fetch(
                "u", "h", "/base", ["a"], tempfile.gettempdir())
        self.assertEqual((ok, err), (False, "stop here"))
        self.assertIn(" --hard-dereference ", seen["command"])

    def test_reader_still_refuses_a_link_entry(self):
        # A link entry carries no bytes of its own, so it can never satisfy
        # "this member holds the planned file". The fix is on the producer.
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode="w",
                          format=tarfile.PAX_FORMAT) as archive:
            first = tarfile.TarInfo("orig.txt")
            first.size = 5
            archive.addfile(first, io.BytesIO(b"hello"))
            link = tarfile.TarInfo("link.txt")
            link.type = tarfile.LNKTYPE
            link.linkname = "orig.txt"
            archive.addfile(link)
        with self.assertRaisesRegex(StoredTarError, "unsupported TAR member"):
            _validate(_gnu_pad(output.getvalue()), [
                StoredTarExpectedMember("orig.txt", 5, 0),
                StoredTarExpectedMember("link.txt", 5, 1)])

    def test_dereferenced_pair_validates_as_two_full_members(self):
        entries = [("orig.txt", b"hello"), ("link.txt", b"hello")]
        result = _validate(_regular_tar(entries), [
            StoredTarExpectedMember("orig.txt", 5, 0),
            StoredTarExpectedMember("link.txt", 5, 1)])
        self.assertEqual([m.logical_size for m in result.members], [5, 5])


def _gnu_tar_shell():
    """(bash, env) for a real GNU tar, or None. Git for Windows ships one."""
    candidates = []
    if os.name == "nt":
        for root in (os.environ.get("ProgramFiles"),
                     os.environ.get("ProgramW6432")):
            if root:
                candidates.append(os.path.join(root, "Git", "usr", "bin"))
    else:
        found = shutil.which("bash")
        if found:
            candidates.append(os.path.dirname(found))
    for bin_dir in candidates:
        bash = os.path.join(bin_dir, "bash.exe" if os.name == "nt" else "bash")
        if not os.path.isfile(bash):
            continue
        env = dict(os.environ)
        env["PATH"] = bin_dir + os.pathsep + env.get("PATH", "")
        try:
            version = subprocess.run(
                [bash, "-c", "tar --version"], env=env, capture_output=True,
                timeout=30).stdout
        except (OSError, subprocess.SubprocessError):
            continue
        if b"GNU tar" in version:
            return bash, env
    return None


@unittest.skipUnless(_gnu_tar_shell(), "needs a local GNU tar")
class RealGnuTarProducerTests(unittest.TestCase):
    """The pinned producer command, run for real on synthetic files."""

    def _produce(self, command_for):
        bash, env = _gnu_tar_shell()
        with tempfile.TemporaryDirectory() as root:
            base = os.path.join(root, "base")
            os.makedirs(base)
            with open(os.path.join(base, "trail.bin"), "wb") as handle:
                handle.write(b"DATA")
                handle.truncate(1048576)            # data, then zeros to EOF
            with open(os.path.join(base, "allhole.bin"), "wb") as handle:
                handle.truncate(1048576)
            with open(os.path.join(base, "orig.txt"), "wb") as handle:
                handle.write(b"hello")
            os.link(os.path.join(base, "orig.txt"),
                    os.path.join(base, "link.txt"))
            names = ["trail.bin", "allhole.bin", "orig.txt", "link.txt"]
            done = subprocess.run(
                [bash, "-c", command_for(base.replace("\\", "/"))],
                input="".join(f"{name}\0" for name in names).encode(),
                env=env, capture_output=True, timeout=120)
            self.assertEqual(done.returncode, 0, done.stderr)
            return done.stdout

    PLAN = [StoredTarExpectedMember("trail.bin", 1048576, 0),
            StoredTarExpectedMember("allhole.bin", 1048576, 1),
            StoredTarExpectedMember("orig.txt", 5, 2),
            StoredTarExpectedMember("link.txt", 5, 3)]

    def test_pinned_command_output_passes_the_strict_reader(self):
        archive = self._produce(_stored_tar_command)
        result = _validate(archive, self.PLAN)
        by_name = {member.name: member for member in result.members}
        self.assertTrue(by_name["trail.bin"].sparse)
        self.assertTrue(by_name["allhole.bin"].sparse)
        self.assertLess(by_name["trail.bin"].stored_size, 1048576)
        self.assertFalse(by_name["link.txt"].sparse)
        self.assertEqual(by_name["link.txt"].stored_size, 5)
        # The second hard-linked name really carries its own bytes.
        with tarfile.open(fileobj=io.BytesIO(archive)) as opened:
            self.assertEqual(
                opened.extractfile("link.txt").read(), b"hello")

    def test_without_the_option_gnu_tar_emits_the_rejected_link_entry(self):
        archive = self._produce(
            lambda base: _stored_tar_command(base).replace(
                " --hard-dereference", ""))
        with self.assertRaisesRegex(StoredTarError, "unsupported TAR member"):
            _validate(archive, self.PLAN)


if __name__ == "__main__":
    unittest.main()
