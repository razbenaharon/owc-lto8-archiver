"""The one exception type that means "refused on purpose", not "crashed".

Maintenance commands refuse for reasons an operator is *supposed* to hit: the
archiver lock is held, a transfer is running, a required flag is missing, the
archive root is misconfigured.  Those are answers, not bugs, and printing a
Python traceback for them buries the one line that matters under a stack the
operator cannot act on.

Only refusals raise :class:`OperationalError`.  Everything else — a ``KeyError``
from a renamed column, a psycopg failure, an unhandled ``RuntimeError`` — keeps
propagating with its full traceback, because that *is* the debugging evidence.
The CLI therefore never needs a blanket ``except Exception``: the exception type
itself carries the distinction between "expected refusal" and "genuine bug".

It subclasses ``RuntimeError`` so existing ``except RuntimeError`` handlers
(``inspect_db._open_db``, the orchestrators) keep behaving exactly as before.
"""


class OperationalError(RuntimeError):
    """An expected, operator-facing refusal. Print the message; exit 1."""
