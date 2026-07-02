"""Catalog path helpers used by the PostgreSQL archive browser."""
import re


_DRIVE_RE = re.compile(r"^[A-Za-z]:")


def normalize_catalog_path(path):
    """Normalize a stored/source path for catalog browsing keys."""
    value = str(path or "").replace("\\", "/").strip()
    value = re.sub(r"/+", "/", value)
    if value != "/" and value.endswith("/"):
        value = value.rstrip("/")
    return value


def _split_path(path):
    value = normalize_catalog_path(path)
    if not value:
        return []
    if _DRIVE_RE.match(value):
        drive = value[:2]
        rest = value[2:].strip("/")
        return [drive] + ([p for p in rest.split("/") if p] if rest else [])
    if value.startswith("/"):
        rest = value.strip("/")
        return ["/"] + ([p for p in rest.split("/") if p] if rest else [])
    return [p for p in value.split("/") if p]


def _short_host(value):
    value = (value or "").strip()
    if not value:
        return "local"
    return value.split(".", 1)[0]


def _root_parts(file_path, source_host):
    """Replace drive/root markers with stable catalog roots."""
    parts = _split_path(file_path)
    if not parts:
        return parts
    first = parts[0]
    if _DRIVE_RE.match(first):
        return ["LOCAL"] + parts[1:]
    if first == "/":
        return [_short_host(source_host)] + parts[1:]
    return parts


def catalog_file_name(stored_path=None, original_path=None):
    """Return the display/search filename used by the catalog indexes."""
    for candidate in (stored_path, original_path):
        parts = _split_path(candidate)
        if parts:
            leaf = parts[-1]
            if leaf != "/" and not _DRIVE_RE.match(leaf):
                return leaf
    return ""


def catalog_directory_chain(file_path, source_host=None):
    """Return directory rows as (normalized_path, parent_path, name)."""
    parts = _root_parts(file_path, source_host)
    if len(parts) <= 1:
        return []

    chain = []
    current = None
    for index, part in enumerate(parts[:-1]):
        normalized = part if index == 0 else current + "/" + part
        parent = current
        name = normalized if parent is None else part
        chain.append((normalized, parent, name))
        current = normalized
    return chain
