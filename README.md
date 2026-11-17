# Lockstep

An **ACID-flavored in-memory key-value store** built for teaching
concurrency control: strict **two-phase locking** with a real
wait-for-graph deadlock detector, a **snapshot-isolation** mode as an
alternative, conflicts surfaced to the caller as typed errors, and a
**deterministic scheduler hook** so tests can interleave transactions
explicitly — no threads, no sleeps, no flakiness.

**Zero dependencies** — Python standard library only.

## API in one screen

```python
from txndb import KVStore, LockWait, DeadlockError, SnapshotConflict

db = KVStore(mode="2pl")          # or mode="si"

t = db.begin()                    # t.state == "active"
t.get("k")                        # S lock (2pl) / snapshot read (si)
t.set("k", "v")                   # X lock or S->X upgrade (2pl), buffered
t.commit()                        # applies writes, releases locks
t.rollback()                      # restores prior versions

db.read("k")                      # non-transactional committed read
db.dump()                         # committed state as a dict
db.lock_info()                    # {key: {txn_id: "S"|"X"}}  ({} under SI)
db.scheduler.trace()              # deterministic execution log
```

## Strict two-phase locking (mode `"2pl"`)

* Reads take **shared (S)** locks, writes take **exclusive (X)**
  locks; a reader may **upgrade** `S → X` in place when nobody else
  holds the key.
* **Strict**: every lock is held until commit/rollback — not until the
  last use — so no dirty reads, no uncommitted reads, no lost updates.
* Writes are buffered until commit; `db.read`/`db.dump` only ever show
  committed state.  `rollback` discards the buffer (restoring the prior
  versions of every touched key) and releases all locks.
* **Blocked requests surface to the caller**: a lock that cannot be
  granted right now raises `LockWait` (a `ConflictError`).  Nothing is
  retried silently.
* **Deadlock detection via the wait-for graph**: registering a wait
  builds edges `waiter → conflicting holders`; when a cycle closes, the
  **victim is the youngest transaction (highest id)** — deterministic —
  it is rolled back automatically (locks released, writes discarded)
  and the caller receives `DeadlockError`.  Detection re-runs until the
  graph is acyclic, so chained cycles resolve too.

## Snapshot isolation (mode `"si"`) — and its anomaly

No locks at all.  Each transaction reads its **snapshot** (versions
committed at `begin` time) plus its own buffered writes; commits use
**first-committer-wins**: writing a key that someone else committed
after your snapshot raises `SnapshotConflict` at commit time.

SI cannot dirty-read (writes stay buffered) but **allows write skew** —
two transactions reading different keys and writing different keys both
commit, breaking an invariant that neither could break alone.  The test
`test_write_skew_visible_as_documented_si_anomaly` demonstrates exactly
this, and the 2PL mode prevents it (the second writer blocks).

## Deterministic scheduler hook

Every operation is announced to `db.scheduler`: the event lands in the
execution trace, then the optional `on_event` callback runs.  When a
transaction blocks (`kind == "lock_wait"`) the hook fires *inside* the
blocked call — commit, roll back, or run other transactions' operations
right there — and the blocked operation re-checks its lock afterwards.
Tests therefore script exact interleavings and assert the exact trace:

```python
db = KVStore(mode="2pl")
t1, t2 = db.begin(), db.begin()
t2.set("k", 5)

def hook(event):
    if event.kind == "lock_wait" and event.txn_id == t1.id:
        t2.commit()                      # interleave: holder finishes

db.scheduler.on_event = hook
t1.get("k")                              # -> 5, no exception
db.scheduler.trace()
# [('begin', 1, None, 'ok'), ('begin', 2, None, 'ok'),
#  ('set', 2, 'k', 'ok'), ('lock_wait', 1, 'k', 'wait'),
#  ('commit', 2, None, 'ok'), ('get', 1, 'k', 'ok')]
```

## Runnable example

`demo.py` in this repository (output below):

```console
> python demo.py
alice reads: 100
bob reads:   100
alice blocked: transaction 2 cannot write 'balance' now (upgrade blocked by another reader)
deadlock: transaction 3 was chosen as the deadlock victim and rolled back
victim state: aborted
final balance: 70
locks after commit: {}
t2 snapshot still sees x = 1
write skew result: {'x': 0, 'y': 0} (x + y = 0: anomaly visible)
```

## Error model

| exception            | mode | meaning                                        |
|----------------------|------|------------------------------------------------|
| `LockWait`           | 2pl  | lock not grantable now; transaction still active |
| `DeadlockError`      | 2pl  | this transaction was the victim (auto-rolled back) |
| `SnapshotConflict`   | si   | write/write conflict at commit (first committer wins) |
| `TxnError`           | both | misuse: op on committed/aborted/foreign txn     |

`LockWait`, `DeadlockError` and `SnapshotConflict` all subclass
`ConflictError` — every conflict is **surfaced to the caller**.

## Design notes / limitations

* Single-process, in-memory; locks are per-key and granted on release
  without a FIFO queue (a teaching-store simplification — no
  queue-fairness guarantee).
* SI keeps committed versions forever (no garbage collection below the
  oldest active snapshot).
* The wait-for graph is rebuilt on demand from pending requests —
  O(transactions) per detection, which is exactly what you want to show
  in a lecture.

## Test command

```
python -m unittest discover -s tests -v
```

23 tests: dirty reads impossible, shared-lock compatibility, locks
held until commit (strict 2PL), lost update prevented via the upgrade
deadlock, wait-for-graph cycle detection + deterministic youngest-victim
selection (unit-tested for 2- and 3-transaction cycles), rollback
restoring prior versions and releasing locks, exact scheduler traces
for scripted interleavings and hook-driven interleaving *inside* a
blocked call, SI snapshot reads, buffered no-lock reads, read-your-
writes, write-skew demonstrated with the broken invariant, first-
committer-wins conflicts, and transaction state misuse.
