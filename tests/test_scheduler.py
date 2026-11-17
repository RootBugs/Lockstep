"""Deterministic scheduler hook + wait-for graph units."""
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from txndb import DeadlockError, KVStore, LockWait, LockManager  # noqa: E402
from txndb.scheduler import Event, Scheduler  # noqa: E402


class TestSchedulerHook(unittest.TestCase):
    def test_on_event_interleaves_during_lock_wait(self):
        """The hook runs *inside* the blocked call: committing the
        holder there lets the blocked read complete without raising."""
        db = KVStore(mode="2pl")
        t1 = db.begin()
        t2 = db.begin()
        t2.set("k", 5)

        def hook(event: Event):
            if event.kind == "lock_wait" and event.txn_id == t1.id:
                t2.commit()  # deterministic interleave: holder finishes

        db.scheduler.on_event = hook
        self.assertEqual(t1.get("k"), 5)  # granted after the interleave
        self.assertEqual(
            db.scheduler.trace(),
            [
                ("begin", t1.id, None, "ok"),
                ("begin", t2.id, None, "ok"),
                ("set", t2.id, "k", "ok"),
                ("lock_wait", t1.id, "k", "wait"),
                ("commit", t2.id, None, "ok"),
                ("get", t1.id, "k", "ok"),
            ],
        )
        t1.rollback()

    def test_history_is_deterministic_for_scripted_interleaving(self):
        """Explicit transaction interleaving produces an exact,
        reproducible execution trace (the lost-update scenario)."""
        db = KVStore(mode="2pl")
        seed = db.begin()
        seed.set("k", 0)
        seed.commit()
        db.scheduler.clear()

        t1 = db.begin()
        t2 = db.begin()
        v1 = t1.get("k")
        v2 = t2.get("k")
        with self.assertRaises(LockWait):
            t1.set("k", v1 + 1)
        with self.assertRaises(DeadlockError):
            t2.set("k", v2 + 1)
        t1.set("k", v1 + 1)
        t1.commit()
        # t2 was already rolled back automatically as the victim

        one, two = t1.id, t2.id
        self.assertEqual(
            db.scheduler.trace(),
            [
                ("begin", one, None, "ok"),
                ("begin", two, None, "ok"),
                ("get", one, "k", "ok"),
                ("get", two, "k", "ok"),
                ("lock_wait", one, "k", "wait"),
                ("lock_wait", two, "k", "wait"),
                ("deadlock", two, "k", "deadlock"),
                ("set", one, "k", "ok"),
                ("commit", one, None, "ok"),
            ],
        )
        self.assertEqual(db.read("k"), 1)

    def test_aborted_transaction_rollback_emits_event(self):
        db = KVStore(mode="2pl")
        t = db.begin()
        t.set("k", 1)
        t.rollback()
        self.assertEqual(
            db.scheduler.trace()[-1], ("rollback", t.id, None, "ok")
        )

    def test_trace_and_clear_helpers(self):
        sched = Scheduler()
        sched.emit("begin", 1)
        sched.emit("get", 1, "k", "wait")
        self.assertEqual(
            sched.trace(),
            [("begin", 1, None, "ok"), ("get", 1, "k", "wait")],
        )
        sched.clear()
        self.assertEqual(sched.trace(), [])


class TestWaitForGraph(unittest.TestCase):
    def test_cycle_detection_and_victim_selection(self):
        locks = LockManager()
        # t1 and t2 both hold S on 'k' (compatible), both want X
        locks.acquire(1, "k", "S")
        locks.acquire(2, "k", "S")
        locks.set_pending(1, "k", "X")
        # only one waiter: no cycle yet
        self.assertEqual(locks.find_cycle(), [])
        locks.set_pending(2, "k", "X")
        cycle = locks.find_cycle()
        self.assertEqual(sorted(cycle), [1, 2])
        # deterministic victim: the youngest (highest id)
        self.assertEqual(locks.choose_victim(cycle), 2)
        # aborting the victim breaks the cycle
        locks.release_all(2)
        self.assertEqual(locks.find_cycle(), [])

    def test_wait_for_edges_follow_conflicts(self):
        locks = LockManager()
        locks.acquire(1, "a", "X")
        locks.acquire(2, "b", "S")
        locks.set_pending(3, "a", "S")  # waits for t1's X
        locks.set_pending(4, "b", "X")  # waits for t2's S
        edges = locks.wait_for_edges()
        self.assertEqual(edges, {3: {1}, 4: {2}})
        self.assertEqual(locks.find_cycle(), [])
        # incompatible grants are reported per transaction
        self.assertEqual(locks.conflicting_holders(3, "a", "S"), [1])
        self.assertEqual(locks.conflicting_holders(1, "a", "X"), [])

    def test_three_transaction_cycle_resolves_repeatedly(self):
        locks = LockManager()
        for tid in (1, 2, 3):
            locks.acquire(tid, f"k{tid}", "S")
        # ring: 1 waits for 2, 2 waits for 3, 3 waits for 1
        locks.set_pending(1, "k2", "X")
        locks.set_pending(2, "k3", "X")
        locks.set_pending(3, "k1", "X")
        cycle = locks.find_cycle()
        self.assertEqual(sorted(cycle), [1, 2, 3])
        victim = locks.choose_victim(cycle)
        self.assertEqual(victim, 3)
        locks.release_all(victim)
        self.assertEqual(locks.find_cycle(), [])


if __name__ == "__main__":
    unittest.main()
