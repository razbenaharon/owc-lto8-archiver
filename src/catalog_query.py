"""Shared SQL helpers for catalog search (LIKE/ILIKE pattern building).

Centralizes the escaping rules so the CLI search path (``pg_db``) and the GUI
inspector (``inspector_repository``) cannot drift apart — historically they did,
and only one of them escaped LIKE metacharacters.

User-facing wildcards are ``*`` (any run) and ``?`` (single char). Every other
character — including SQL LIKE's own metacharacters ``%`` and ``_`` — is treated
literally, which matters because ``_`` is extremely common in real filenames.
All patterns are intended to be used with ``ESCAPE '\\'`` in the SQL text.
"""

LIKE_ESCAPE = "\\"


def escape_like_literal(term):
    """Escape LIKE metacharacters so ``term`` matches literally.

    The escape character is doubled first, then ``%`` and ``_`` are escaped.
    ``*``/``?`` are intentionally left intact so callers can translate them into
    wildcards afterwards.
    """
    return (str(term)
            .replace(LIKE_ESCAPE, LIKE_ESCAPE * 2)
            .replace("%", LIKE_ESCAPE + "%")
            .replace("_", LIKE_ESCAPE + "_"))


def contains_pattern(term):
    """Build an ILIKE pattern for a user filename query.

    ``*`` becomes ``%`` and ``?`` becomes ``_``. When the user supplies no
    wildcard the (escaped) term is wrapped as a substring match; otherwise the
    translated pattern is used as-is (anchored). Literal ``%``/``_`` in the term
    match themselves.
    """
    term = str(term)
    has_wildcard = "*" in term or "?" in term
    pattern = escape_like_literal(term).replace("*", "%").replace("?", "_")
    return pattern if has_wildcard else f"%{pattern}%"


def prefix_pattern(term):
    """Build an ILIKE pattern that matches ``term`` as a literal prefix."""
    return escape_like_literal(term) + "%"


def substring_pattern(term):
    """Build an ILIKE pattern that matches ``term`` as a literal substring."""
    return f"%{escape_like_literal(term)}%"
