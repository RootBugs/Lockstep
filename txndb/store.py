"""txndb: transactional in-memory key-value store.

Two concurrency-control modes behind one API::

    db = KVStore(mode="2pl")   # strict two-phase locking (default)
    db = KVStore(mode="si")    # snapshot isolation

    t = db.begin()
    t.get(key) / t.set(key, value)
    t.commit() / t.rollback()

**Strict 2PL** (mode ``"2pl"``): reads take shared (S) locks, writes
take exclusive (X) locks, an S lock can be upgraded to X, and *every*
lock is held until commit/rollback — so dirty reads and lost updates
are impossible.  Blocked requests surface to the caller as
:class:`~txndb.errors.LockWait`; if registering the wait closes a cycle
in the wait-for graph the youngest transaction is chosen as the
**deadlock victim**, rolled back automatically, and reported as
:class:`~txndb.errors.DeadlockError`.

**Snapshot isolation** (mode ``"si"``): no locks at all.  Each
transaction reads its snapshot (versions committed at begin time plus
its own buffered writes), writes are buffered, and commits use
first-committer-wins: writing a key someone else committed after your
snapshot raises :class:`~txndb.errors.SnapshotConflict`.  SI therefore
allows the classic **write-skew** anomaly (documented and demonstrated
in the tests) which 2PL forbids.

Writes in both modes are buffered until commit, so ``db.read`` /
``db.dump`` always show committed state only.
"""
from __future__ import annotations

from .errors import (
    ConflictError,
    DeadlockError,
    LockWait,
    SnapshotConflict,
    TxnError,
)
from .locks import EXCLUSIVE, SHARED, LockManager
from .scheduler import Scheduler

MODES = ("2pl", "si")


class Txn:
    """A transaction handle: get/set + commit/rollback."""

    def __init__(self, store: "KVStore", txn_id: int, start_ts: int) -> None:
        self._store = store
        self.id = txn_id
        self.start_ts = start_ts
        self.state = "active"  # active | committed | aborted

    def get(self, key: str):
        return self._store._get(self, key)

    def set(self, key: str, value) -> None:
        self._store._set(self, key, value)

    def commit(self) -> None:
        self._store.commit(self)

    def rollback(self) -> None:
        self._store.rollback(self)

    @property
    def writes(self) -> dict:
        """Buffered (uncommitted) writes of this transaction."""
        return dict(self._store._writes.get(self.id, {}))

    def __repr__(self) -> str:
        return f"<Txn {self.id} {self.state}>"


class KVStore:
    """Transactional key-value store (``mode='2pl'`` or ``mode='si'``)."""

    def __init__(self, mode: str = "2pl", scheduler: Scheduler | None = None) -> None:
        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}, got {mode!r}")
        self.mode = mode
        self.scheduler = scheduler if scheduler is not None else Scheduler()
        self.locks = LockManager()
        self._committed: dict[str, object] = {}       # 2pl committed view
        self._versions: dict[str, list[tuple[int, object]]] = {}  # si
        self._commit_ts = 0
        self._writes: dict[int, dict[str, object]] = {}
        self._txns: dict[int, Txn] = {}
        self._next_id = 1

    # ------------------------------------------------------- lifecycle
    def begin(self) -> Txn:
        txn = Txn(self, self._next_id, self._commit_ts)
        self._next_id += 1
        self._txns[txn.id] = txn
        self.scheduler.emit("begin", txn.id)
        return txn

    def commit(self, txn: Txn) -> None:
        self._require_active(txn)
        writes = self._writes.pop(txn.id, {})
        if self.mode == "si":
            conflict = self._find_write_conflict(txn, writes)
            if conflict is not None:
                txn.state = "aborted"
                self.scheduler.emit("commit", txn.id, conflict, "conflict")
                raise SnapshotConflict(
                    f"transaction {txn.id} aborted: {conflict!r} was "
                    f"committed by another transaction after its snapshot "
                    f"(first committer wins)"
                )
            self._commit_ts += 1
            ts = self._commit_ts
            for key, value in writes.items():
                self._versions.setdefault(key, []).append((ts, value))
        else:
            self._committed.update(writes)
            self.locks.release_all(txn.id)
        txn.state = "committed"
        self.scheduler.emit("commit", txn.id)

    def rollback(self, txn: Txn) -> None:
        self._require_active(txn)
        self._writes.pop(txn.id, None)
        if self.mode == "2pl":
            self.locks.release_all(txn.id)
        txn.state = "aborted"
        self.scheduler.emit("rollback", txn.id)

    # ------------------------------------------------------ inspection
    def read(self, key: str):
        """Non-transactional read of committed state (no locks taken)."""
        if self.mode == "si":
            versions = self._versions.get(key)
            return versions[-1][1] if versions else None
        return self._committed.get(key)

    def dump(self) -> dict:
        """Committed state as a plain dict."""
        if self.mode == "si":
            return {k: v[-1][1] for k, v in self._versions.items()}
        return dict(self._committed)

    def lock_info(self) -> dict:
        """Current lock grants (``{key: {txn_id: mode}}``); {} under SI."""
        return self.locks.snapshot() if self.mode == "2pl" else {}

    def state_of(self, txn: Txn) -> str:
        return txn.state

    # ---------------------------------------------------------- reads
    def _get(self, txn: Txn, key: str):
        self._require_active(txn)
        own = self._writes.get(txn.id, {})
        if key in own:
            self.scheduler.emit("get", txn.id, key)
            return own[key]
        if self.mode == "si":
            value = self._si_read(txn, key)
            self.scheduler.emit("get", txn.id, key)
            return value
        # 2pl: shared lock, blocking through the scheduler hook
        if not self._acquire(txn, key, SHARED):
            raise LockWait(
                f"transaction {txn.id} cannot read {key!r} now "
                f"(held exclusively by another transaction)"
            )
        self.scheduler.emit("get", txn.id, key)
        return self._committed.get(key)

    # --------------------------------------------------------- writes
    def _set(self, txn: Txn, key: str, value) -> None:
        self._require_active(txn)
        if self.mode == "2pl":
            held = self.locks.mode_of(txn.id, key)
            if held != EXCLUSIVE:
                # fresh X request, or an S -> X upgrade
                if not self._acquire(txn, key, EXCLUSIVE):
                    raise LockWait(
                        f"transaction {txn.id} cannot write {key!r} now"
                        + (" (upgrade blocked by another reader)"
                           if held == SHARED else "")
                    )
        self._writes.setdefault(txn.id, {})[key] = value
        self.scheduler.emit("set", txn.id, key)

    # ------------------------------------------------------- internals
    def _acquire(self, txn: Txn, key: str, mode: str) -> bool:
        """Try to grant ``mode`` on ``key`` to ``txn``.

        Returns True when granted.  On a block: emit ``lock_wait`` (the
        scheduler hook may interleave other transactions), retry once,
        then register the wait-for edge and resolve any deadlock —
        aborting victims — before finally raising ``LockWait``.
        """
        locks = self.locks
        locks.clear_pending(txn.id)  # stale waits are abandoned on progress
        if locks.can_acquire(txn.id, key, mode):
            locks.acquire(txn.id, key, mode)
            return True
        self.scheduler.emit("lock_wait", txn.id, key, "wait")
        if locks.can_acquire(txn.id, key, mode):  # hook may have freed it
            locks.acquire(txn.id, key, mode)
            return True
        return self._wait_for(txn, key, mode)

    def _wait_for(self, txn: Txn, key: str, mode: str) -> bool:
        """Register the wait and resolve deadlocks (victim = youngest)."""
        locks = self.locks
        locks.set_pending(txn.id, key, mode)
        while True:
            cycle = locks.find_cycle()
            if not cycle:
                return False  # caller raises LockWait
            victim_id = locks.choose_victim(cycle)
            if victim_id == txn.id:
                self._abort(txn)
                self.scheduler.emit("deadlock", txn.id, key, "deadlock")
                raise DeadlockError(
                    f"transaction {txn.id} was chosen as the deadlock "
                    f"victim and rolled back"
                )
            victim = self._txns[victim_id]
            self._abort(victim)
            self.scheduler.emit("deadlock", victim_id, key, "deadlock")
            if locks.can_acquire(txn.id, key, mode):
                locks.clear_pending(txn.id)
                locks.acquire(txn.id, key, mode)
                return True
            # still blocked by someone else: loop and re-detect

    def _abort(self, txn: Txn) -> None:
        """Roll back without emitting a rollback event (internal)."""
        self._writes.pop(txn.id, None)
        if self.mode == "2pl":
            self.locks.release_all(txn.id)
        txn.state = "aborted"

    def _si_read(self, txn: Txn, key: str):
        for ts, value in reversed(self._versions.get(key, ())):
            if ts <= txn.start_ts:
                return value
        return None

    def _find_write_conflict(self, txn: Txn, writes: dict) -> str | None:
        """First-committer-wins check: any key changed since snapshot?"""
        for key in writes:
            for ts, _value in reversed(self._versions.get(key, ())):
                if ts > txn.start_ts:
                    return key
        return None

    def _require_active(self, txn: Txn) -> None:
        if not isinstance(txn, Txn) or txn._store is not self:
            raise TxnError("transaction does not belong to this store")
        if txn.state != "active":
            raise TxnError(
                f"transaction {txn.id} is {txn.state}; only active "
                f"transactions can be used"
            )


__all__ = [
    "ConflictError",
    "DeadlockError",
    "KVStore",
    "LockWait",
    "Scheduler",
    "SnapshotConflict",
    "Txn",
    "TxnError",
]
