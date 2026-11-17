"""Lock manager: shared/exclusive locks, upgrades, wait-for graph.

Model
-----
* Each key holds a set of ``{txn_id: "S" | "X"}`` grants.  ``S`` is
  shared, ``X`` exclusive; an ``X`` holder has the key alone.
* A transaction may hold multiple keys at once and keeps every grant
  until commit/rollback (**strict** two-phase locking).
* ``acquire(txn, key, "S")`` conflicts with any *other* holder of ``X``;
  ``acquire(txn, key, "X")`` conflicts with any other holder of ``S``
  or ``X``.  A transaction re-acquiring a key it already holds upgrades
  ``S -> X`` in place when nobody else holds it.
* When a request cannot be granted, the caller registers a *pending*
  request.  The **wait-for graph** has an edge
  ``waiter -> each conflicting holder``; a cycle is a deadlock.
* **Victim selection** is deterministic: the transaction with the
  highest id (the youngest) in any cycle is chosen as the victim and
  rolled back by the store.  Detection re-runs until the graph is
  acyclic, so chained cycles resolve too.

Fairness: requests are granted on release without queueing (no FIFO
queue) — a teaching-store simplification, noted in the README.
"""
from __future__ import annotations

from .errors import TxnError

SHARED = "S"
EXCLUSIVE = "X"


def modes_conflict(held: str, requested: str) -> bool:
    """True when ``held`` by another txn blocks ``requested``."""
    return EXCLUSIVE in (held, requested)


class LockManager:
    """Tracks key grants and pending requests for one store."""

    def __init__(self) -> None:
        self._held: dict[str, dict[int, str]] = {}
        self._pending: dict[int, tuple[str, str]] = {}

    # ---------------------------------------------------------- grants
    def held_by(self, key: str) -> dict[int, str]:
        """txn_id -> mode for everyone currently holding ``key``."""
        return dict(self._held.get(key, {}))

    def mode_of(self, txn_id: int, key: str) -> str | None:
        return self._held.get(key, {}).get(txn_id)

    def locks_of(self, txn_id: int) -> dict[str, str]:
        """key -> mode for every grant held by ``txn_id``."""
        out: dict[str, str] = {}
        for key, holders in self._held.items():
            if txn_id in holders:
                out[key] = holders[txn_id]
        return out

    def conflicting_holders(self, txn_id: int, key: str, mode: str) -> list[int]:
        """Other transactions whose current grant blocks this request."""
        result = []
        for other, held in self._held.get(key, {}).items():
            if other != txn_id and modes_conflict(held, mode):
                result.append(other)
        return sorted(result)

    def can_acquire(self, txn_id: int, key: str, mode: str) -> bool:
        return not self.conflicting_holders(txn_id, key, mode)

    def acquire(self, txn_id: int, key: str, mode: str) -> None:
        """Grant (or upgrade) — caller must have checked :meth:`can_acquire`."""
        holders = self._held.setdefault(key, {})
        current = holders.get(txn_id)
        if current == EXCLUSIVE:
            return  # already exclusive on this key
        if current == SHARED and mode == SHARED:
            return  # already shared
        if current == SHARED and mode == EXCLUSIVE:
            holders[txn_id] = EXCLUSIVE  # in-place upgrade
            return
        holders[txn_id] = mode

    # --------------------------------------------------------- waiting
    def pending_of(self, txn_id: int) -> tuple[str, str] | None:
        return self._pending.get(txn_id)

    def set_pending(self, txn_id: int, key: str, mode: str) -> None:
        self._pending[txn_id] = (key, mode)

    def clear_pending(self, txn_id: int) -> None:
        self._pending.pop(txn_id, None)

    def wait_for_edges(self) -> dict[int, set[int]]:
        """waiter -> set of transactions it currently waits for."""
        edges: dict[int, set[int]] = {}
        for txn_id, (key, mode) in self._pending.items():
            edges[txn_id] = set(self.conflicting_holders(txn_id, key, mode))
        return edges

    def find_cycle(self) -> list[int]:
        """Return the members of a wait-for cycle, or ``[]``.

        Deterministic: nodes are visited in id order and the cycle is
        returned starting from its smallest id.
        """
        edges = self.wait_for_edges()
        color: dict[int, int] = {}  # 0 = unvisited, 1 = on stack, 2 = done
        stack: list[int] = []
        found: list[int] = []

        def dfs(node: int) -> bool:
            color[node] = 1
            stack.append(node)
            for nxt in sorted(edges.get(node, ())):
                state = color.get(nxt, 0)
                if state == 1:
                    # cycle from nxt .. end of stack
                    idx = stack.index(nxt)
                    found.extend(stack[idx:])
                    return True
                if state == 0 and dfs(nxt):
                    return True
            color[node] = 2
            stack.pop()
            return False

        for node in sorted(edges):
            if color.get(node, 0) == 0:
                stack.clear()
                if dfs(node):
                    break
        return found

    def choose_victim(self, cycle: list[int]) -> int:
        """Deterministic victim: the youngest (highest id) member."""
        if not cycle:
            raise TxnError("cannot choose a victim from an empty cycle")
        return max(cycle)

    # --------------------------------------------------------- release
    def release_all(self, txn_id: int) -> list[str]:
        """Drop every grant and pending request of ``txn_id``.

        Returns the keys that were released.
        """
        released = []
        for key in [k for k, h in self._held.items() if txn_id in h]:
            del self._held[key][txn_id]
            if not self._held[key]:
                del self._held[key]
            released.append(key)
        self.clear_pending(txn_id)
        return released

    def snapshot(self) -> dict[str, dict[int, str]]:
        """Full grant table (keys -> {txn: mode}) for inspection/tests."""
        return {k: dict(v) for k, v in self._held.items()}
