"""Deterministic scheduler hook.

Every store operation is announced to the scheduler: the event is
appended to :attr:`Scheduler.history` (an auditable execution trace)
and then passed to the optional :attr:`Scheduler.on_event` callback.

The callback is what makes interleavings *deterministic*: when a
transaction blocks (``kind == "lock_wait"``) the hook runs — and may
perform other transactions' operations (commit, rollback, read, write)
right there, inside the blocked call.  When the hook returns, the
blocked operation re-checks its lock: if the interleaved work granted
it, the operation completes normally instead of raising ``LockWait``.

History events are plain tuples of
``(kind, txn_id, key, status)`` where ``status`` is one of
``ok`` / ``wait`` / ``deadlock``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class Event:
    kind: str  # begin | get | set | commit | rollback | lock_wait | deadlock
    txn_id: int
    key: str | None = None
    status: str = "ok"

    def as_tuple(self) -> tuple:
        return (self.kind, self.txn_id, self.key, self.status)


class Scheduler:
    """Records events and lets tests inject deterministic interleavings."""

    def __init__(self) -> None:
        self.history: list[Event] = []
        self.on_event: Callable[[Event], None] | None = None

    def emit(self, kind: str, txn_id: int, key: str | None = None,
             status: str = "ok") -> Event:
        event = Event(kind, txn_id, key, status)
        self.history.append(event)
        if self.on_event is not None:
            self.on_event(event)
        return event

    def trace(self) -> list[tuple]:
        """History as plain tuples, convenient for assertions."""
        return [e.as_tuple() for e in self.history]

    def clear(self) -> None:
        self.history.clear()
