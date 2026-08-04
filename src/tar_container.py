"""Strict, non-extracting reader for the first Stored TAR dialect.

This module is intentionally a consumer only.  It does not create archives,
touch LTFS, or attempt to repair uncertain input.  The parser reads file data
in bounded chunks and retains only the typed member records it must return.
"""
from __future__ import annotations

import os
import posixpath
import re
from contextlib import nullcontext
from dataclasses import replace
from typing import BinaryIO, Iterable, Mapping, Optional

from .pipeline_types import (
    ContainerFormat,
    FileTransferStatus,
    StoredTarContainer,
    StoredTarExpectedMember,
    StoredTarMember,
)


STORED_TAR_READER_CONTRACT_VERSION = 1
STORED_TAR_FORMAT_VERSION = "stored-tar-v1"
STORED_TAR_DIALECT = "gnu-pax-sparse-v1"

BLOCK_SIZE = 512
GNU_BLOCKING_FACTOR = 512
GNU_RECORD_SIZE = BLOCK_SIZE * GNU_BLOCKING_FACTOR
_ZERO_BLOCK = bytes(BLOCK_SIZE)
_COPY_BUFFER_SIZE = 1024 * 1024
_MAX_PAX_PAYLOAD = 8 * 1024 * 1024
_MAX_SPARSE_NUMBER_LENGTH = 64
_DRIVE_PREFIX = re.compile(r"^[A-Za-z]:")

_REGULAR_TYPES = frozenset((b"\0", b"0"))
_PAX_TYPES = frozenset((b"x", b"g"))
_PAX_KEYS = frozenset({
    "path", "size", "mtime", "atime", "ctime", "uid", "gid",
    "uname", "gname", "charset", "comment",
    "GNU.sparse.major", "GNU.sparse.minor", "GNU.sparse.name",
    "GNU.sparse.realsize",
})
_GLOBAL_PAX_KEYS = frozenset({
    "mtime", "atime", "ctime", "uid", "gid", "uname", "gname",
    "charset", "comment",
})
_SPARSE_KEYS = frozenset({
    "GNU.sparse.major", "GNU.sparse.minor", "GNU.sparse.name",
    "GNU.sparse.realsize",
})
_ABSENT_SOURCE_EXCEPTIONS = frozenset({
    FileTransferStatus.SOURCE_MISSING,
    FileTransferStatus.SOURCE_PERMISSION_DENIED,
    FileTransferStatus.SOURCE_UNREADABLE,
})


class StoredTarError(ValueError):
    """The container cannot be proven to satisfy the Stored TAR contract."""


def validate_tar_member_name(name: str) -> str:
    """Return a normalized safe POSIX member name, or reject it.

    Rejection is deliberate: this validator never drops or rewrites unsafe
    components.  A literal backslash *inside* a relative Linux name remains a
    legal character; a leading backslash, UNC prefix, or drive prefix does not.
    """
    if not isinstance(name, str):
        raise StoredTarError("TAR member name must be text")
    if not name:
        raise StoredTarError("TAR member name is empty")
    if "\0" in name:
        raise StoredTarError("TAR member name contains NUL")
    try:
        name.encode("utf-8", "strict")
    except UnicodeError as exc:
        raise StoredTarError("TAR member name is not valid UTF-8 text") from exc
    if name.startswith(("/", "\\")):
        raise StoredTarError(f"absolute TAR member name is forbidden: {name!r}")
    if name.startswith("//") or name.startswith("\\\\"):
        raise StoredTarError(f"UNC TAR member name is forbidden: {name!r}")
    if _DRIVE_PREFIX.match(name):
        raise StoredTarError(
            f"drive-qualified TAR member name is forbidden: {name!r}")

    parts = name.split("/")
    if any(part in (".", "..") for part in parts):
        raise StoredTarError(
            f"dot traversal component in TAR member name: {name!r}")
    if parts[-1] == "":
        raise StoredTarError(f"TAR member name ends with '/': {name!r}")

    normalized = posixpath.normpath(name)
    if normalized in ("", ".", "..") or normalized.startswith("../"):
        raise StoredTarError(f"unsafe TAR member name: {name!r}")
    return normalized


def _mapping_value(record, *keys, default=None):
    if isinstance(record, Mapping):
        for key in keys:
            if key in record:
                return record[key]
        return default
    for key in keys:
        if hasattr(record, key):
            return getattr(record, key)
    return default


def _coerce_expected(record) -> StoredTarExpectedMember:
    if isinstance(record, StoredTarExpectedMember):
        item = record
    else:
        is_exception_record = (
            isinstance(record, Mapping)
            and record.get("record_type") == "source_exception")
        name = _mapping_value(
            record, "name", "member_name",
            *(("path",) if not is_exception_record else ()))
        size = _mapping_value(
            record, "logical_size", "size", "file_size_bytes")
        ordinal = _mapping_value(
            record, "ordinal", "plan_ordinal", "scan_ordinal")
        exception = _mapping_value(
            record, "source_exception", "source_status", default=None)
        if exception is None and is_exception_record:
            exception = _mapping_value(
                record, "disposition", "status", default=None)
        if size is None:
            size = _mapping_value(
                record, "expected_size", "expected_logical_size")
        if size is None or ordinal is None:
            raise StoredTarError(
                "expected member requires logical size and ordinal")
        if name is None and exception is None:
            raise StoredTarError(
                "an expected archived member requires an exact member name")
        item = StoredTarExpectedMember(
            name=None if name is None else str(name),
            logical_size=int(size), ordinal=int(ordinal),
            source_exception=exception)

    try:
        size = int(item.logical_size)
        ordinal = int(item.ordinal)
    except (TypeError, ValueError) as exc:
        raise StoredTarError(
            "expected member size and ordinal must be integers") from exc
    if size < 0 or ordinal < 0:
        raise StoredTarError(
            "expected member size and ordinal must be non-negative")

    exception = item.source_exception
    if exception is not None:
        try:
            exception = FileTransferStatus(exception)
        except (TypeError, ValueError) as exc:
            raise StoredTarError(
                f"unknown source exception for ordinal {ordinal}") from exc
        if exception not in _ABSENT_SOURCE_EXCEPTIONS:
            raise StoredTarError(
                f"source status {exception.value!r} cannot excuse an absent "
                f"TAR member at ordinal {ordinal}")
    return replace(
        item, name=None if item.name is None else str(item.name),
        logical_size=size, ordinal=ordinal,
        source_exception=exception)


def _prepare_plan(expected_members, expected_member_count,
                  expected_logical_bytes):
    plan = tuple(_coerce_expected(item) for item in expected_members)
    previous_ordinal = None
    names = set()
    folded = {}
    present = []
    for item in plan:
        if previous_ordinal is not None and item.ordinal <= previous_ordinal:
            raise StoredTarError("expected member ordinals must strictly ascend")
        previous_ordinal = item.ordinal
        if item.name is not None:
            normalized = validate_tar_member_name(item.name)
            if normalized in names:
                raise StoredTarError(
                    f"duplicate normalized name in expected plan: {item.name!r}")
            names.add(normalized)
            folded_name = normalized.casefold()
            if folded_name in folded:
                raise StoredTarError(
                    "case-fold collision in expected plan: "
                    f"{folded[folded_name]!r} and {item.name!r}")
            folded[folded_name] = item.name
        if item.source_exception is None:
            if item.name is None:
                raise StoredTarError(
                    "an expected archived member requires an exact member name")
            present.append(item)

    plan_count = len(present)
    plan_bytes = sum(item.logical_size for item in present)
    if expected_member_count is not None \
            and int(expected_member_count) != plan_count:
        raise StoredTarError(
            "expected member-count aggregate disagrees with the plan")
    if expected_logical_bytes is not None \
            and int(expected_logical_bytes) != plan_bytes:
        raise StoredTarError(
            "expected logical-byte aggregate disagrees with the plan")
    return tuple(present), plan_count, plan_bytes


class _CountingInput:
    def __init__(self, raw: BinaryIO):
        self.raw = raw
        self.offset = 0

    def read_exact(self, size: int, what: str) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            data = self.raw.read(size - len(chunks))
            if not data:
                raise StoredTarError(
                    f"truncated TAR while reading {what} at byte {self.offset}")
            chunks.extend(data)
            self.offset += len(data)
        return bytes(chunks)

    def read_some(self, size: int) -> bytes:
        data = self.raw.read(size)
        if data:
            self.offset += len(data)
        return data

    def skip_exact(self, size: int, what: str):
        remaining = size
        while remaining:
            data = self.read_exact(min(remaining, _COPY_BUFFER_SIZE), what)
            remaining -= len(data)

    def read_zeroes(self, size: int, what: str):
        remaining = size
        while remaining:
            data = self.read_exact(min(remaining, _COPY_BUFFER_SIZE), what)
            if any(data):
                raise StoredTarError(f"{what} is not all-zero")
            remaining -= len(data)


def _parse_octal(field: bytes, label: str, *, allow_base256=True) -> int:
    if allow_base256 and field and field[0] & 0x80:
        # POSIX base-256: clear the representation flag and interpret the
        # remaining value as a non-negative big-endian integer.  Negative TAR
        # numeric values have no place in the Stored TAR contract.
        if field[0] & 0x40:
            raise StoredTarError(f"negative TAR {label} field")
        data = bytearray(field)
        data[0] &= 0x7f
        value = int.from_bytes(data, "big", signed=False)
        return value
    stripped = field.strip(b" \0")
    if not stripped:
        return 0
    if any(byte < ord("0") or byte > ord("7") for byte in stripped):
        raise StoredTarError(f"invalid TAR {label} field")
    return int(stripped, 8)


def _parse_text_field(field: bytes, label: str) -> str:
    nul = field.find(b"\0")
    if nul >= 0:
        if any(field[nul + 1:]):
            raise StoredTarError(f"embedded NUL/nonzero tail in TAR {label}")
        field = field[:nul]
    try:
        return field.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise StoredTarError(f"invalid UTF-8 bytes in TAR {label}") from exc


def _parse_header(block: bytes):
    if len(block) != BLOCK_SIZE:
        raise StoredTarError("internal error: TAR header is not one block")
    stored_checksum = _parse_octal(
        block[148:156], "checksum", allow_base256=False)
    checksum_block = block[:148] + (b" " * 8) + block[156:]
    unsigned_checksum = sum(checksum_block)
    signed_checksum = sum(
        byte if byte < 128 else byte - 256 for byte in checksum_block)
    if stored_checksum not in (unsigned_checksum, signed_checksum):
        raise StoredTarError("corrupt TAR header checksum")
    if block[257:263] != b"ustar\0" or block[263:265] != b"00":
        raise StoredTarError("TAR header is not POSIX PAX/ustar version 00")
    if any(block[500:512]):
        raise StoredTarError("nonzero reserved bytes in TAR header")

    # Parse every numeric structural field so malformed-but-checksummed input
    # does not pass merely because the reader does not use its ownership data.
    for field, label in (
            (block[100:108], "mode"), (block[108:116], "uid"),
            (block[116:124], "gid"), (block[124:136], "size"),
            (block[136:148], "mtime"), (block[329:337], "devmajor"),
            (block[337:345], "devminor")):
        _parse_octal(field, label)

    name = _parse_text_field(block[0:100], "name")
    prefix = _parse_text_field(block[345:500], "prefix")
    if prefix:
        name = f"{prefix}/{name}"
    linkname = _parse_text_field(block[157:257], "link name")
    return {
        "name": name,
        "size": _parse_octal(block[124:136], "size"),
        "type": block[156:157],
        "linkname": linkname,
    }


def _parse_decimal(value: str, label: str) -> int:
    if not value or not value.isascii() or not value.isdecimal():
        raise StoredTarError(f"invalid decimal PAX {label}")
    result = int(value, 10)
    if result < 0:
        raise StoredTarError(f"negative PAX {label}")
    return result


def _parse_pax_payload(payload: bytes, *, global_header: bool):
    records = {}
    position = 0
    while position < len(payload):
        space = payload.find(b" ", position)
        if space < 0:
            raise StoredTarError("malformed PAX record length")
        length_text = payload[position:space]
        if (not length_text or len(length_text) > 20
                or any(byte < ord("0") or byte > ord("9")
                       for byte in length_text)):
            raise StoredTarError("malformed PAX record length")
        record_length = int(length_text)
        if record_length <= space - position + 3:
            raise StoredTarError("invalid PAX record length")
        end = position + record_length
        if end > len(payload) or payload[end - 1:end] != b"\n":
            raise StoredTarError("truncated or unterminated PAX record")
        body = payload[space + 1:end - 1]
        equals = body.find(b"=")
        if equals <= 0:
            raise StoredTarError("malformed PAX key/value record")
        try:
            key = body[:equals].decode("ascii", "strict")
            value = body[equals + 1:].decode("utf-8", "strict")
        except UnicodeDecodeError as exc:
            raise StoredTarError("invalid text encoding in PAX record") from exc
        if "\0" in value:
            raise StoredTarError(f"NUL in PAX value {key!r}")
        allowed = _GLOBAL_PAX_KEYS if global_header else _PAX_KEYS
        if key not in allowed:
            raise StoredTarError(f"unsupported PAX key: {key!r}")
        if key in records:
            raise StoredTarError(f"duplicate PAX key: {key!r}")
        records[key] = value
        position = end
    return records


def _read_pax(stream: _CountingInput, size: int, *, global_header: bool):
    if size == 0:
        raise StoredTarError("empty PAX metadata header")
    if size > _MAX_PAX_PAYLOAD:
        raise StoredTarError(
            f"PAX metadata payload exceeds {_MAX_PAX_PAYLOAD} bytes")
    payload = stream.read_exact(size, "PAX metadata")
    padding = (-size) % BLOCK_SIZE
    stream.read_zeroes(padding, "PAX metadata padding")
    return _parse_pax_payload(payload, global_header=global_header)


def _read_sparse_line(stream: _CountingInput, remaining: int, label: str):
    value = bytearray()
    consumed = 0
    while consumed < remaining:
        byte = stream.read_exact(1, f"GNU sparse {label}")
        consumed += 1
        if byte == b"\n":
            try:
                text = value.decode("ascii", "strict")
            except UnicodeDecodeError as exc:
                raise StoredTarError(
                    f"invalid GNU sparse {label}") from exc
            return _parse_decimal(text, f"GNU sparse {label}"), consumed
        if len(value) >= _MAX_SPARSE_NUMBER_LENGTH:
            raise StoredTarError(f"GNU sparse {label} is too long")
        value.extend(byte)
    raise StoredTarError(f"truncated GNU sparse {label}")


def _consume_sparse_payload(stream: _CountingInput, physical_size: int,
                            logical_size: int):
    remaining = physical_size
    extent_count, used = _read_sparse_line(stream, remaining, "extent count")
    remaining -= used
    previous_end = 0
    stored_data_bytes = 0
    for index in range(extent_count):
        offset, used = _read_sparse_line(
            stream, remaining, f"extent {index} offset")
        remaining -= used
        length, used = _read_sparse_line(
            stream, remaining, f"extent {index} length")
        remaining -= used
        if length <= 0:
            raise StoredTarError("GNU sparse extents must have positive length")
        if offset < previous_end or offset + length > logical_size:
            raise StoredTarError("invalid or overlapping GNU sparse extent")
        previous_end = offset + length
        stored_data_bytes += length

    map_bytes = physical_size - remaining
    map_padding = (-map_bytes) % BLOCK_SIZE
    if map_padding > remaining:
        raise StoredTarError("truncated GNU sparse map padding")
    stream.read_zeroes(map_padding, "GNU sparse map padding")
    remaining -= map_padding
    if remaining != stored_data_bytes:
        raise StoredTarError(
            "GNU sparse payload size disagrees with its extent map")
    stream.skip_exact(remaining, "GNU sparse stored extents")
    stream.read_zeroes(
        (-physical_size) % BLOCK_SIZE, "TAR member data padding")
    return extent_count


class StoredTarReader:
    """Streaming, non-extracting validator for one Stored TAR container."""

    def __init__(self, source, *, tar_dialect: str, format_version: str):
        if tar_dialect != STORED_TAR_DIALECT:
            raise StoredTarError(
                f"unsupported Stored TAR dialect: {tar_dialect!r}")
        if format_version != STORED_TAR_FORMAT_VERSION:
            raise StoredTarError(
                f"unsupported Stored TAR format version: {format_version!r}")
        self.source = source
        self.tar_dialect = tar_dialect
        self.format_version = format_version

    def parse(self, expected_members: Iterable,
              *, expected_member_count: Optional[int] = None,
              expected_logical_bytes: Optional[int] = None):
        return self.validate(
            expected_members,
            expected_member_count=expected_member_count,
            expected_logical_bytes=expected_logical_bytes)

    def read(self, expected_members: Iterable,
             *, expected_member_count: Optional[int] = None,
             expected_logical_bytes: Optional[int] = None):
        return self.validate(
            expected_members,
            expected_member_count=expected_member_count,
            expected_logical_bytes=expected_logical_bytes)

    def validate(self, expected_members: Iterable,
                 *, expected_member_count: Optional[int] = None,
                 expected_logical_bytes: Optional[int] = None):
        present, plan_count, plan_bytes = _prepare_plan(
            expected_members, expected_member_count, expected_logical_bytes)
        if isinstance(self.source, (str, bytes, os.PathLike)):
            context = open(self.source, "rb")
        else:
            if not hasattr(self.source, "read"):
                raise TypeError("Stored TAR source must be a path or binary stream")
            context = nullcontext(self.source)
        with context as raw:
            result = self._validate_stream(raw, present, plan_count, plan_bytes)
        return result

    def _validate_stream(self, raw, present, plan_count, plan_bytes):
        stream = _CountingInput(raw)
        global_pax = {}
        pending_pax = None
        members = []
        seen_names = set()
        seen_folded = {}
        member_position = 0

        while True:
            block = stream.read_exact(BLOCK_SIZE, "TAR header/end marker")
            if block == _ZERO_BLOCK:
                second = stream.read_exact(BLOCK_SIZE, "second TAR end block")
                if second != _ZERO_BLOCK:
                    raise StoredTarError(
                        "TAR end marker contains only one zero block")
                if pending_pax is not None:
                    raise StoredTarError(
                        "PAX extended header has no following member")
                break

            header = _parse_header(block)
            member_type = header["type"]
            if member_type in _PAX_TYPES:
                if header["linkname"]:
                    raise StoredTarError("PAX metadata header has a link target")
                records = _read_pax(
                    stream, header["size"], global_header=member_type == b"g")
                if member_type == b"g":
                    global_pax.update(records)
                else:
                    if pending_pax is not None:
                        raise StoredTarError(
                            "multiple PAX extended headers precede one member")
                    pending_pax = records
                continue
            if member_type not in _REGULAR_TYPES:
                label = member_type.decode("ascii", "backslashreplace")
                raise StoredTarError(
                    f"unsupported TAR member type {label!r}")
            if header["linkname"]:
                raise StoredTarError("regular TAR member has a link target")

            pax = dict(global_pax)
            if pending_pax:
                pax.update(pending_pax)
            pending_pax = None
            sparse_keys = _SPARSE_KEYS.intersection(pax)
            is_sparse = bool(sparse_keys)
            if is_sparse and sparse_keys != _SPARSE_KEYS:
                raise StoredTarError("incomplete GNU sparse v1.0 metadata")

            if is_sparse:
                if pax["GNU.sparse.major"] != "1" \
                        or pax["GNU.sparse.minor"] != "0":
                    raise StoredTarError("unsupported GNU sparse version")
                if "size" in pax:
                    raise StoredTarError(
                        "ambiguous standard size on GNU sparse v1.0 member")
                name = pax["GNU.sparse.name"]
                if "path" in pax and pax["path"] != name:
                    raise StoredTarError(
                        "PAX path disagrees with GNU sparse member name")
                logical_size = _parse_decimal(
                    pax["GNU.sparse.realsize"], "GNU sparse real size")
                physical_size = header["size"]
            else:
                name = pax.get("path", header["name"])
                logical_size = (_parse_decimal(pax["size"], "size")
                                if "size" in pax else header["size"])
                physical_size = logical_size

            normalized = validate_tar_member_name(name)
            if normalized in seen_names:
                raise StoredTarError(
                    f"duplicate normalized TAR member name: {name!r}")
            folded = normalized.casefold()
            if folded in seen_folded:
                raise StoredTarError(
                    "case-fold collision in TAR members: "
                    f"{seen_folded[folded]!r} and {name!r}")
            seen_names.add(normalized)
            seen_folded[folded] = name

            if member_position >= len(present):
                raise StoredTarError(f"unexpected TAR member: {name!r}")
            expected = present[member_position]
            if name != expected.name:
                later = next(
                    (item for item in present[member_position + 1:]
                     if item.name == name), None)
                if later is not None:
                    raise StoredTarError(
                        f"wrong member ordinal/order: {name!r} at archive "
                        f"position {member_position}, planned ordinal "
                        f"{later.ordinal}")
                raise StoredTarError(
                    f"unexpected TAR member {name!r}; expected "
                    f"{expected.name!r} at ordinal {expected.ordinal}")
            if logical_size != expected.logical_size:
                raise StoredTarError(
                    f"wrong size for {name!r}: archived {logical_size}, "
                    f"expected {expected.logical_size}")

            if is_sparse:
                extent_count = _consume_sparse_payload(
                    stream, physical_size, logical_size)
            else:
                stream.skip_exact(physical_size, "TAR member data")
                stream.read_zeroes(
                    (-physical_size) % BLOCK_SIZE,
                    "TAR member data padding")
                extent_count = 0
            members.append(StoredTarMember(
                name=name, normalized_name=normalized,
                logical_size=logical_size, stored_size=physical_size,
                ordinal=expected.ordinal, sparse=is_sparse,
                sparse_extent_count=extent_count))
            member_position += 1

        if member_position != len(present):
            missing = present[member_position]
            raise StoredTarError(
                f"missing TAR member without source exception: "
                f"{missing.name!r} at ordinal {missing.ordinal}")

        # GNU `-b 512` writes exactly enough all-zero padding to finish the
        # current 512-block record.  Extra all-zero records are not accepted:
        # they could conceal a concatenated empty archive.
        blocking_padding = (-stream.offset) % GNU_RECORD_SIZE
        stream.read_zeroes(blocking_padding, "GNU TAR blocking padding")
        if stream.read_some(1):
            raise StoredTarError(
                "nonzero, extra-zero, or concatenated data follows TAR padding")

        observed_count = len(members)
        observed_bytes = sum(item.logical_size for item in members)
        if observed_count != plan_count:
            raise StoredTarError("TAR member-count aggregate mismatch")
        if observed_bytes != plan_bytes:
            raise StoredTarError("TAR logical-byte aggregate mismatch")
        return StoredTarContainer(
            container_format=ContainerFormat.STORED_TAR,
            format_version=self.format_version,
            tar_dialect=self.tar_dialect,
            members=tuple(members), member_count=observed_count,
            logical_bytes=observed_bytes, archive_size=stream.offset)


def validate_stored_tar(source, expected_members: Iterable, *,
                        tar_dialect: str, format_version: str,
                        expected_member_count: Optional[int] = None,
                        expected_logical_bytes: Optional[int] = None):
    """Validate an entire Stored TAR against its plan/sidecar expectations."""
    return StoredTarReader(
        source, tar_dialect=tar_dialect,
        format_version=format_version).validate(
            expected_members,
            expected_member_count=expected_member_count,
            expected_logical_bytes=expected_logical_bytes)
