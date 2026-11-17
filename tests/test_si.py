"""Snapshot isolation: snapshots, no locks, write-skew, WW conflicts."""
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from txndb import KVStore, SnapshotConflict, TxnError  # noqa: E402


class SITestCase(unittest.TestCase):
    def setUp(self):
        self.db = KVStore(mode="si")

    def seed(self, **pairs):
        t = self.db.begin()
        for key, value in pairs.items():
            t.set(key, value)
        t.commit()


class TestSnapshotIsolation(SITestCase):
    def test_snapshot_ignores_concurrent_commits(self):
        self.seed(x=1)
        reader = self.db.begin()      # snapshot taken now
        writer = self.db.begin()
        writer.set("x", 2)
        writer.commit()
        # the already-open transaction still sees its snapshot
        self.assertEqual(reader.get("x"), 1)
        # a fresh transaction sees the new committed value
        fresh = self.db.begin()
        self.assertEqual(fresh.get("x"), 2)
        reader.rollback()
        fresh.rollback()

    def test_no_locks_and_no_dirty_reads(self):
        """SI buffers writes: readers are never blocked and never see
        uncommitted data (contrast with the 2pl LockWait behavior)."""
        self.seed(k="old")
        writer = self.db.begin()
        reader = self.db.begin()
        writer.set("k", "uncommitted")  # buffered, not applied
        self.assertEqual(reader.get("k"), "old")  # no exception, no wait
        self.assertEqual(self.db.read("k"), "old")  # committed view only
        self.assertEqual(self.db.lock_info(), {})  # SI takes no locks
        writer.rollback()
        self.assertEqual(reader.get("k"), "old")
        reader.rollback()

    def test_read_your_writes_before_commit(self):
        t = self.db.begin()
        t.set("k", 1)
        self.assertEqual(t.get("k"), 1)
        t.set("k", 2)
        self.assertEqual(t.get("k"), 2)
        self.assertEqual(self.db.read("k"), None)  # invisible outside
        t.commit()
        self.assertEqual(self.db.read("k"), 2)

    def test_rollback_discards_buffered_writes(self):
        self.seed(k="kept")
        t = self.db.begin()
        t.set("k", "discarded")
        t.set("new", "discarded")
        t.rollback()
        self.assertEqual(t.state, "aborted")
        self.assertEqual(self.db.dump(), {"k": "kept"})
        with self.assertRaises(TxnError):
            t.commit()

    def test_write_skew_visible_as_documented_si_anomaly(self):
        """Classic write-skew: two transactions read 'at least one of
        x,y is 1', each zeroes its own key, and both commit — the
        invariant x + y >= 1 breaks.  2PL would have blocked the second
        writer; SI allows it, and this test pins that anomaly down."""
        self.seed(x=1, y=1)
        t1 = self.db.begin()
        t2 = self.db.begin()
        # both read the same snapshot: x=1, y=1 → predicate holds
        self.assertEqual((t1.get("x"), t1.get("y")), (1, 1))
        self.assertEqual((t2.get("x"), t2.get("y")), (1, 1))
        t1.set("x", 0)     # each transaction writes a DIFFERENT key,
        t2.set("y", 0)     # so no write/write conflict is detected
        t1.commit()        # both commit cleanly...
        t2.commit()
        self.assertEqual(t1.state, "committed")
        self.assertEqual(t2.state, "committed")
        # ...and the invariant is violated: write skew is visible.
        self.assertEqual(self.db.dump(), {"x": 0, "y": 0})
        self.assertLess(self.db.read("x") + self.db.read("y"), 1)

    def test_write_write_conflict_first_committer_wins(self):
        self.seed(k="base")
        t1 = self.db.begin()
        t2 = self.db.begin()
        t1.set("k", "from-t1")
        t1.commit()
        t2.set("k", "from-t2")  # t2's snapshot predates t1's commit
        with self.assertRaises(SnapshotConflict) as ctx:
            t2.commit()
        self.assertIn("first committer wins", str(ctx.exception))
        self.assertEqual(t2.state, "aborted")
        self.assertEqual(self.db.read("k"), "from-t1")  # loser's write dropped
        # the aborted transaction left nothing behind
        self.assertEqual(t2.writes, {})

    def test_mode_validation(self):
        with self.assertRaises(ValueError):
            KVStore(mode="nope")
        self.assertEqual(KVStore(mode="si").mode, "si")
        self.assertEqual(KVStore(mode="2pl").mode, "2pl")


if __name__ == "__main__":
    unittest.main()
