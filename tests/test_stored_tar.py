"""Strict Stored TAR consumer tests.  No producer, SSH, database, or LTFS."""
import io
import os
import tarfile
import tempfile
import tracemalloc
import unittest

from src.pipeline_types import FileTransferStatus, StoredTarExpectedMember
from src.tar_container import (
    GNU_RECORD_SIZE,
    STORED_TAR_DIALECT,
    STORED_TAR_FORMAT_VERSION,
    StoredTarError,
    StoredTarReader,
    validate_stored_tar,
    validate_tar_member_name,
)


def _gnu_pad(data):
    return data + bytes((-len(data)) % GNU_RECORD_SIZE)


def _regular_tar(entries, *, archive_format=tarfile.PAX_FORMAT,
                 pax_headers=None):
    output = io.BytesIO()
    with tarfile.open(
            fileobj=output, mode="w", format=archive_format,
            pax_headers=pax_headers) as archive:
        for name, data in entries:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return _gnu_pad(output.getvalue())


def _typed_plan(entries, *, start=0):
    return [
        StoredTarExpectedMember(name, len(data), start + index)
        for index, (name, data) in enumerate(entries)
    ]


def _validate(data, plan, **kwargs):
    return validate_stored_tar(
        io.BytesIO(data), plan,
        tar_dialect=kwargs.pop("tar_dialect", STORED_TAR_DIALECT),
        format_version=kwargs.pop(
            "format_version", STORED_TAR_FORMAT_VERSION),
        **kwargs)


def _set_checksum(header):
    header[148:156] = b"        "
    checksum = sum(header)
    header[148:156] = f"{checksum:06o}\0 ".encode("ascii")


def _mutate_first_header(data, mutate):
    result = bytearray(data)
    header = result[:512]
    mutate(header)
    _set_checksum(header)
    result[:512] = header
    return bytes(result)


def _pax_record(key, value):
    body = f" {key}={value}\n".encode("utf-8")
    length = len(body) + 1
    while True:
        encoded = str(length).encode("ascii") + body
        if len(encoded) == length:
            return encoded
        length = len(encoded)


def _sparse_tar(name, logical_size, extents, *, major="1", minor="0"):
    sparse_map = [str(len(extents)).encode("ascii") + b"\n"]
    stored = bytearray()
    for offset, data in extents:
        sparse_map.extend((
            str(offset).encode("ascii") + b"\n",
            str(len(data)).encode("ascii") + b"\n"))
        stored.extend(data)
    map_data = b"".join(sparse_map)
    payload = map_data + bytes((-len(map_data)) % 512) + bytes(stored)
    info = tarfile.TarInfo("GNUSparseFile.1/member")
    info.size = len(payload)
    info.pax_headers = {
        "GNU.sparse.major": major,
        "GNU.sparse.minor": minor,
        "GNU.sparse.name": name,
        "GNU.sparse.realsize": str(logical_size),
    }
    headers = info.tobuf(format=tarfile.PAX_FORMAT)
    archive = headers + payload + bytes((-len(payload)) % 512)
    archive += bytes(1024)
    return _gnu_pad(archive)


def _special_tar(name, member_type):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.PAX_FORMAT) as arc:
        info = tarfile.TarInfo(name)
        info.type = member_type
        if member_type in (tarfile.SYMTYPE, tarfile.LNKTYPE):
            info.linkname = "target"
        if member_type in (tarfile.CHRTYPE, tarfile.BLKTYPE):
            info.devmajor = 1
            info.devminor = 2
        arc.addfile(info)
    return _gnu_pad(output.getvalue())


class NameValidationTests(unittest.TestCase):
    def test_accepts_special_but_safe_names(self):
        for name in (
                "space name", "tab\tname", "line\nname", "snowman-☃",
                r"literal\backslash", "-leading-dash", "a//b"):
            with self.subTest(name=name):
                self.assertTrue(validate_tar_member_name(name))

    def test_rejects_absolute_drive_unc_and_traversal_names(self):
        for name in (
                "/absolute", r"\rooted", r"\\server\share", "C:/drive",
                r"C:\drive", "C:relative", "../up", "a/../up", "./file",
                "a/./file", "a/..", "a/", ""):
            with self.subTest(name=name):
                with self.assertRaises(StoredTarError):
                    validate_tar_member_name(name)

    def test_rejects_nul_and_invalid_unicode(self):
        for name in ("bad\0name", "bad\udcffname"):
            with self.subTest(name=repr(name)):
                with self.assertRaises(StoredTarError):
                    validate_tar_member_name(name)


class DialectAndRegularMemberTests(unittest.TestCase):
    def test_exact_dialect_and_version_match(self):
        entries = [("one.txt", b"one")]
        result = _validate(_regular_tar(entries), _typed_plan(entries))
        self.assertEqual(result.tar_dialect, STORED_TAR_DIALECT)
        self.assertEqual(result.format_version, STORED_TAR_FORMAT_VERSION)
        self.assertEqual(result.member_count, 1)
        self.assertEqual(result.logical_bytes, 3)
        self.assertEqual(result.archive_size, GNU_RECORD_SIZE)

    def test_persisted_dialect_mismatch_is_rejected(self):
        entries = [("one.txt", b"one")]
        with self.assertRaisesRegex(StoredTarError, "dialect"):
            _validate(
                _regular_tar(entries), _typed_plan(entries),
                tar_dialect="posix-pax-default")

    def test_persisted_format_version_mismatch_is_rejected(self):
        entries = [("one.txt", b"one")]
        with self.assertRaisesRegex(StoredTarError, "format version"):
            _validate(
                _regular_tar(entries), _typed_plan(entries),
                format_version="stored-tar-v2")

    def test_non_pax_gnu_header_dialect_is_rejected(self):
        entries = [("one.txt", b"one")]
        with self.assertRaisesRegex(StoredTarError, "PAX/ustar"):
            _validate(
                _regular_tar(entries, archive_format=tarfile.GNU_FORMAT),
                _typed_plan(entries))

    def test_regular_files_and_stable_ordinals(self):
        entries = [("a", b""), ("nested/b", b"payload")]
        result = _validate(
            _regular_tar(entries), _typed_plan(entries, start=40),
            expected_member_count=2, expected_logical_bytes=7)
        self.assertEqual([item.ordinal for item in result.members], [40, 41])
        self.assertEqual([item.logical_size for item in result.members], [0, 7])
        self.assertTrue(all(not item.sparse for item in result.members))

    def test_pax_long_unicode_and_unusual_names(self):
        names = [
            "long/" + ("segment-" * 20) + "end.txt",
            "δοκιμή/雪.txt",
            "spaces tabs\tand\nnewlines",
            r"literal\backslash",
            "-leading-dash",
        ]
        entries = [(name, str(index).encode()) for index, name in enumerate(names)]
        result = _validate(_regular_tar(entries), _typed_plan(entries))
        self.assertEqual([item.name for item in result.members], names)

    def test_reader_accepts_a_nonseekable_binary_stream(self):
        class NonSeekable(io.BytesIO):
            def seekable(self):
                return False

            def seek(self, *args, **kwargs):
                raise io.UnsupportedOperation("streaming fixture")

        entries = [("one", b"data")]
        reader = StoredTarReader(
            NonSeekable(_regular_tar(entries)),
            tar_dialect=STORED_TAR_DIALECT,
            format_version=STORED_TAR_FORMAT_VERSION)
        self.assertEqual(reader.read(_typed_plan(entries)).member_count, 1)


class SparseMemberTests(unittest.TestCase):
    def test_gnu_pax_sparse_v1_member(self):
        logical_size = 512 * 1024 * 1024
        data = _sparse_tar(
            "sparse/image.bin", logical_size,
            [(0, b"abc"), (logical_size - 2, b"xy")])
        plan = [StoredTarExpectedMember(
            "sparse/image.bin", logical_size, 7)]
        result = _validate(data, plan)
        member = result.members[0]
        self.assertTrue(member.sparse)
        self.assertEqual(member.sparse_extent_count, 2)
        self.assertEqual(member.logical_size, logical_size)
        self.assertLess(member.stored_size, 1024)

    def test_sparse_version_mismatch_is_rejected(self):
        data = _sparse_tar("sparse", 100, [(5, b"x")], major="0", minor="1")
        with self.assertRaisesRegex(StoredTarError, "sparse version"):
            _validate(data, [StoredTarExpectedMember("sparse", 100, 0)])

    def test_sparse_overlap_is_rejected(self):
        data = _sparse_tar("sparse", 100, [(5, b"123"), (7, b"x")])
        with self.assertRaisesRegex(StoredTarError, "overlapping"):
            _validate(data, [StoredTarExpectedMember("sparse", 100, 0)])


class PlanEquivalenceTests(unittest.TestCase):
    def test_duplicate_member_is_rejected(self):
        entries = [("same", b"a"), ("same", b"a")]
        plan = [StoredTarExpectedMember("same", 1, 0)]
        with self.assertRaisesRegex(StoredTarError, "duplicate"):
            _validate(_regular_tar(entries), plan)

    def test_duplicate_normalized_name_is_rejected(self):
        entries = [("a//b", b"a"), ("a/b", b"b")]
        plan = [
            StoredTarExpectedMember("a//b", 1, 0),
            StoredTarExpectedMember("a/b", 1, 1),
        ]
        with self.assertRaisesRegex(StoredTarError, "duplicate normalized"):
            _validate(_regular_tar(entries), plan)

    def test_windows_casefold_collision_is_rejected(self):
        entries = [("Folder/File", b"a"), ("folder/file", b"b")]
        plan = [
            StoredTarExpectedMember("Folder/File", 1, 0),
            StoredTarExpectedMember("folder/file", 1, 1),
        ]
        with self.assertRaisesRegex(StoredTarError, "case-fold"):
            _validate(_regular_tar(entries), plan)

    def test_missing_member_without_exception_is_rejected(self):
        entries = [("present", b"yes")]
        plan = _typed_plan(entries) + [
            StoredTarExpectedMember("missing", 8, 1)]
        with self.assertRaisesRegex(StoredTarError, "missing TAR member"):
            _validate(_regular_tar(entries), plan)

    def test_missing_member_with_explicit_source_exception_is_accepted(self):
        entries = [("present", b"yes")]
        plan = _typed_plan(entries) + [StoredTarExpectedMember(
            "missing", 8, 1, FileTransferStatus.SOURCE_MISSING)]
        result = _validate(
            _regular_tar(entries), plan,
            expected_member_count=1, expected_logical_bytes=3)
        self.assertEqual(result.member_count, 1)

    def test_sidecar_shaped_source_exception_needs_no_invented_member_name(self):
        result = _validate(_regular_tar([]), [{
            "record_type": "source_exception",
            "expected_size": 8,
            "plan_ordinal": 3,
            "disposition": "source_missing",
        }])
        self.assertEqual(result.member_count, 0)

    def test_only_supported_explicit_source_exceptions_can_excuse_absence(self):
        for status in (
                FileTransferStatus.SOURCE_PERMISSION_DENIED,
                FileTransferStatus.SOURCE_UNREADABLE):
            with self.subTest(status=status):
                result = _validate(
                    _regular_tar([]),
                    [StoredTarExpectedMember("absent", 9, 0, status)])
                self.assertEqual(result.member_count, 0)
        with self.assertRaisesRegex(StoredTarError, "cannot excuse"):
            _validate(
                _regular_tar([]),
                [StoredTarExpectedMember(
                    "absent", 9, 0, FileTransferStatus.UNRESOLVED)])

    def test_member_present_despite_source_exception_is_unexpected(self):
        entries = [("claimed-missing", b"x")]
        plan = [StoredTarExpectedMember(
            "claimed-missing", 1, 0, FileTransferStatus.SOURCE_MISSING)]
        with self.assertRaisesRegex(StoredTarError, "unexpected"):
            _validate(_regular_tar(entries), plan)

    def test_unexpected_member_is_rejected(self):
        entries = [("planned", b"x"), ("extra", b"y")]
        plan = [StoredTarExpectedMember("planned", 1, 0)]
        with self.assertRaisesRegex(StoredTarError, "unexpected"):
            _validate(_regular_tar(entries), plan)

    def test_wrong_order_is_reported_as_wrong_ordinal(self):
        entries = [("second", b"2"), ("first", b"1")]
        plan = [
            StoredTarExpectedMember("first", 1, 10),
            StoredTarExpectedMember("second", 1, 11),
        ]
        with self.assertRaisesRegex(StoredTarError, "ordinal/order"):
            _validate(_regular_tar(entries), plan)

    def test_wrong_size_is_rejected(self):
        entries = [("file", b"actual")]
        with self.assertRaisesRegex(StoredTarError, "wrong size"):
            _validate(
                _regular_tar(entries),
                [StoredTarExpectedMember("file", 99, 0)])

    def test_declared_count_and_byte_aggregates_must_match_plan(self):
        entries = [("file", b"abc")]
        data = _regular_tar(entries)
        plan = _typed_plan(entries)
        with self.assertRaisesRegex(StoredTarError, "member-count"):
            _validate(data, plan, expected_member_count=2)
        with self.assertRaisesRegex(StoredTarError, "logical-byte"):
            _validate(data, plan, expected_logical_bytes=4)


class UnsafeAndUnsupportedMemberTests(unittest.TestCase):
    def test_unsafe_member_paths_are_rejected_per_member(self):
        names = [
            "/absolute", "C:/drive", r"C:\drive", r"\\server\share",
            "../escape", "safe/../../escape", "safe/./file",
        ]
        for name in names:
            with self.subTest(name=name):
                data = _regular_tar([(name, b"x")])
                with self.assertRaises(StoredTarError):
                    _validate(data, [StoredTarExpectedMember(name, 1, 0)])

    def test_embedded_nul_name_bytes_are_rejected(self):
        entries = [("safe-name", b"x")]
        data = _mutate_first_header(
            _regular_tar(entries),
            lambda header: header.__setitem__(
                slice(0, 100), b"bad\0evil" + bytes(92)))
        with self.assertRaisesRegex(StoredTarError, "embedded NUL"):
            _validate(data, _typed_plan(entries))

    def test_links_devices_fifo_and_directories_are_rejected(self):
        member_types = (
            tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.CHRTYPE,
            tarfile.BLKTYPE, tarfile.FIFOTYPE, tarfile.DIRTYPE,
        )
        for member_type in member_types:
            with self.subTest(member_type=member_type):
                with self.assertRaisesRegex(StoredTarError, "member type"):
                    _validate(
                        _special_tar("special", member_type),
                        [StoredTarExpectedMember("special", 0, 0)])

    def test_unrecognized_member_type_is_rejected(self):
        data = _mutate_first_header(
            _regular_tar([("file", b"x")]),
            lambda header: header.__setitem__(156, ord("V")))
        with self.assertRaisesRegex(StoredTarError, "member type"):
            _validate(data, [StoredTarExpectedMember("file", 1, 0)])

    def test_unrecognized_pax_metadata_is_rejected(self):
        data = _regular_tar(
            [("file", b"x")], pax_headers={"vendor.unknown": "value"})
        with self.assertRaisesRegex(StoredTarError, "unsupported PAX key"):
            _validate(data, [StoredTarExpectedMember("file", 1, 0)])


class StructuralValidationTests(unittest.TestCase):
    def setUp(self):
        self.entries = [("file", b"x")]
        self.plan = _typed_plan(self.entries)
        self.data = _regular_tar(self.entries)

    def test_corrupt_header_is_rejected(self):
        data = bytearray(self.data)
        data[10] ^= 1
        with self.assertRaisesRegex(StoredTarError, "checksum"):
            _validate(bytes(data), self.plan)

    def test_truncated_member_data_is_rejected(self):
        with self.assertRaisesRegex(StoredTarError, "truncated"):
            _validate(self.data[:700], self.plan)

    def test_truncated_end_marker_is_rejected(self):
        # One one-byte member occupies header+one padded data block.  Retain
        # exactly one of the required two following zero blocks.
        with self.assertRaisesRegex(StoredTarError, "truncated"):
            _validate(self.data[:1536], self.plan)

    def test_valid_all_zero_gnu_blocking_padding_is_accepted(self):
        result = _validate(self.data, self.plan)
        self.assertEqual(result.archive_size % GNU_RECORD_SIZE, 0)

    def test_nonzero_member_padding_is_rejected(self):
        data = bytearray(self.data)
        data[513] = 1
        with self.assertRaisesRegex(StoredTarError, "padding"):
            _validate(bytes(data), self.plan)

    def test_nonzero_trailing_bytes_are_rejected(self):
        with self.assertRaisesRegex(StoredTarError, "follows TAR padding"):
            _validate(self.data + b"X", self.plan)

    def test_extra_all_zero_record_is_rejected(self):
        with self.assertRaisesRegex(StoredTarError, "follows TAR padding"):
            _validate(self.data + bytes(GNU_RECORD_SIZE), self.plan)

    def test_concatenated_archives_are_rejected(self):
        second_entries = [("other", b"y")]
        with self.assertRaisesRegex(StoredTarError, "follows TAR padding"):
            _validate(self.data + _regular_tar(second_entries), self.plan)


class BoundedMemoryTests(unittest.TestCase):
    def test_large_stream_is_not_buffered_in_memory(self):
        logical_size = 256 * 1024 * 1024

        class ZeroReader:
            def __init__(self, remaining):
                self.remaining = remaining

            def read(self, size=-1):
                if self.remaining <= 0:
                    return b""
                if size < 0 or size > self.remaining:
                    size = self.remaining
                self.remaining -= size
                return bytes(size)

        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "large.tar")
            with tarfile.open(path, mode="w", format=tarfile.PAX_FORMAT) as arc:
                info = tarfile.TarInfo("large-zero.bin")
                info.size = logical_size
                arc.addfile(info, ZeroReader(logical_size))
            current_size = os.path.getsize(path)
            with open(path, "ab") as handle:
                handle.write(bytes((-current_size) % GNU_RECORD_SIZE))

            tracemalloc.start()
            try:
                result = validate_stored_tar(
                    path,
                    [StoredTarExpectedMember(
                        "large-zero.bin", logical_size, 0)],
                    tar_dialect=STORED_TAR_DIALECT,
                    format_version=STORED_TAR_FORMAT_VERSION)
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            self.assertEqual(result.logical_bytes, logical_size)
            self.assertLess(peak, 12 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
