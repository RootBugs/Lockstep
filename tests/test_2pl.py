"""Strict two-phase locking: isolation, upgrades, deadlock, rollback."""
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from txndb import (  # noqa: E402
    ConflictError,
    DeadlockError,
    KVStore,
    LockWait,
    TxnError,
)


class TwoPhaseTestCase(unittest.TestCase):
    def setUp(self):
        self.db = KVStore(mode="2pl")

    def seed(self, **pairs):
        t = self.db.begin()
        for key, value in pairs.items():
            t.set(key, value)
        t.commit()
        return t


class TestBasicTransactions(TwoPhaseTestCase):
    def test_begin_commit_roundtrip(self):
        self.seed(k="v")
        t = self.db.begin()
        self.assertEqual(t.state, "active")
        self.assertEqual(t.get("k"), "v")
        t.set("k", "v2")
        self.assertEqual(t.get("k"), "v2")  # read your own writes
        t.commit()
        self.assertEqual(t.state, "committed")
        self.assertEqual(self.db.read("k"), "v2")
        self.assertEqual(
            self.db.scheduler.trace()[-2:],
            [("get", t.id, "k", "ok"), ("commit", t.id, None, "ok")],
        )

    def test_rollback_restores_prior_versions_and_releases_locks(self):
        self.seed(a=1, b=2)
        t = self.db.begin()
        t.set("a", 100)
        t.set("b", 200)
        self.assertEqual(t.writes, {"a": 100, "b": 200})
        t.rollback()
        self.assertEqual(t.state, "aborted")
        # prior versions restored for every touched key
        self.assertEqual(self.db.dump(), {"a": 1, "b": 2})
        # every lock was released with the rollback
        self.assertEqual(self.db.lock_info(), {})
        # a new transaction sees the restored values
        t2 = self.db.begin()
        self.assertEqual((t2.get("a"), t2.get("b")), (1, 2))
        t2.rollback()

    def test_transaction_state_misuse_raises(self):
        t = self.db.begin()
        t.commit()
        with self.assertRaises(TxnError):
            t.get("k")
        with self.assertRaises(TxnError):
            t.set("k", 1)
        with self.assertRaises(TxnError):
            t.commit()  # double commit
        with self.assertRaises(TxnError):
            t.rollback()
        # a transaction from another store is rejected too
        other = KVStore(mode="2pl")
        fresh = other.begin()
        with self.assertRaises(TxnError):
            self.db.commit(fresh)


class TestStrictTwoPhaseLocking(TwoPhaseTestCase):
    def test_shared_locks_compatible_exclusive_blocks_readers(self):
        self.seed(k=0)
        t1 = self.db.begin()
        t2 = self.db.begin()
        self.assertEqual(t1.get("k"), 0)  # S
        self.assertEqual(t2.get("k"), 0)  # S + S = compatible
        self.assertEqual(
            self.db.lock_info(), {"k": {t1.id: "S", t2.id: "S"}}
        )
        with self.assertRaises(LockWait):
            t1.set("k", 1)  # upgrade blocked by t2's S
        t2.rollback()
        t1.set("k", 1)  # upgrade now succeeds
        self.assertEqual(self.db.lock_info(), {"k": {t1.id: "X"}})
        t1.commit()

    def test_dirty_read_impossible(self):
        self.seed(k="committed")
        writer = self.db.begin()
        reader = self.db.begin()
        writer.set("k", "uncommitted")
        # the reader cannot see (or even fetch) the uncommitted value
        with self.assertRaises(LockWait) as ctx:
            reader.get("k")
        self.assertIn("cannot read", str(ctx.exception))
        self.assertIsInstance(ctx.exception, ConflictError)
        writer.rollback()
        # after rollback the reader sees only the committed version
        self.assertEqual(reader.get("k"), "committed")
        reader.rollback()

    def test_strict_2pl_holds_locks_until_commit_not_last_use(self):
        self.seed(k=1)
        t1 = self.db.begin()
        t1.set("k", 2)
        # lock stays even though t1 performed no further operation
        self.assertEqual(self.db.lock_info(), {"k": {t1.id: "X"}})
        t2 = self.db.begin()
        with self.assertRaises(LockWait):
            t2.get("k")
        t1.commit()  # only now are the locks released
        self.assertEqual(self.db.lock_info(), {})
        self.assertEqual(t2.get("k"), 2)
        t2.rollback()

    def test_lost_update_prevented(self):
        """Two transactions read the same value, both try to write it:
        the upgrade deadlock is resolved by aborting one writer, so the
        increment is applied exactly once."""
        self.seed(k=0)
        t1 = self.db.begin()
        t2 = self.db.begin()
        v1 = t1.get("k")
        v2 = t2.get("k")
        with self.assertRaises(LockWait):
            t1.set("k", v1 + 1)
        with self.assertRaises(DeadlockError):
            t2.set("k", v2 + 1)  # cycle: both hold S, both want X
        self.assertEqual(t2.state, "aborted")
        t1.set("k", v1 + 1)  # retry succeeds after the victim freed its S
        t1.commit()
        self.assertEqual(self.db.read("k"), 1)  # not 2 — update kept

    def test_write_lock_upgrade_deadlock_detected_and_resolved(self):
        """Classic upgrade deadlock: T1 and T2 each hold S on 'k' and
        each requests X.  The wait-for graph closes a cycle; the
        youngest transaction is the victim."""
        self.seed(k="v")
        t1 = self.db.begin()
        t2 = self.db.begin()
        t1.get("k")
        t2.get("k")
        with self.assertRaises(LockWait) as first:
            t1.set("k", "t1")
        self.assertIn("upgrade blocked", str(first.exception))
        # registering t2's upgrade completes the cycle t1 -> t2 -> t1
        with self.assertRaises(DeadlockError) as second:
            t2.set("k", "t2")
        self.assertIn("victim", str(second.exception))
        self.assertEqual(t2.state, "aborted")  # rolled back automatically
        self.assertEqual(t1.state, "active")  # the older txn survives
        # t2's S lock is gone, so t1's upgrade can complete
        t1.set("k", "t1")
        self.assertEqual(self.db.lock_info(), {"k": {t1.id: "X"}})
        t1.commit()
        self.assertEqual(self.db.read("k"), "t1")

    def test_conflict_errors_are_surfaced_to_caller(self):
        self.seed(k=0)
        t1 = self.db.begin()
        t2 = self.db.begin()
        t1.set("k", 1)
        with self.assertRaises(LockWait) as wait:
            t2.get("k")
        self.assertIsInstance(wait.exception, ConflictError)
        t1.rollback()
        # a second upgrade deadlock surfaces DeadlockError, also a conflict
        s1 = self.db.begin()
        s2 = self.db.begin()
        s1.get("k")
        s2.get("k")
        with self.assertRaises(LockWait):
            s1.set("k", 1)  # blocked — pending registered for the cycle
        with self.assertRaises(DeadlockError) as dead:
            s2.set("k", 2)
        self.assertIsInstance(dead.exception, ConflictError)
        s1.set("k", 1)
        s1.commit()


if __name__ == "__main__":
    unittest.main()
