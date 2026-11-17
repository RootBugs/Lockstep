"""Error types for txndb.

All conflicts surface to the caller as subclasses of
:class:`ConflictError` — nothing is silently retried behind the
caller's back (except deadlock victims, which are aborted *and*
reported).
"""
from __future__ import annotations


class TxnError(Exception):
    """Misuse of a transaction or the store (bad state, bad mode, ...)."""


class ConflictError(Exception):
    """Base class for every concurrency conflict surfaced to the caller."""


class LockWait(ConflictError):
    """A lock request could not be granted *right now*.

    The transaction stays active with the request registered in the
    wait-for graph; the caller may retry after other transactions make
    progress (or use the scheduler hook to interleave them).
    """


class DeadlockError(ConflictError):
    """This transaction was chosen as the deadlock victim.

    The victim is automatically rolled back (locks released, buffered
    writes discarded) before the error is raised, so the store is
    consistent the moment the caller sees this.
    """


class SnapshotConflict(ConflictError):
    """Snapshot-isolation write/write conflict (first committer wins).

    Raised at commit time when another transaction committed a write to
    the same key after this transaction's snapshot was taken.
    """
