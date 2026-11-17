"""Runnable example used by README.md — run: python demo.py"""
import sys

sys.path.insert(0, ".")
from txndb import DeadlockError, KVStore, LockWait

# ---- strict two-phase locking ----------------------------------------
db = KVStore(mode="2pl")
setup = db.begin()
setup.set("balance", 100)
setup.commit()

alice = db.begin()
bob = db.begin()
print("alice reads:", alice.get("balance"))   # 100
print("bob reads:  ", bob.get("balance"))     # 100 (shared locks compatible)

try:
    alice.set("balance", alice.get("balance") - 30)  # needs upgrade to X
except LockWait as exc:
    print("alice blocked:", exc)
try:
    bob.set("balance", bob.get("balance") - 50)      # upgrade -> cycle
except DeadlockError as exc:
    print("deadlock:", exc)
    print("victim state:", bob.state)

alice.set("balance", 70)   # retry succeeds after bob was rolled back
alice.commit()
print("final balance:", db.read("balance"))   # 70 — bob's update was lost
                                                # with the aborted victim
print("locks after commit:", db.lock_info())

# ---- snapshot isolation ------------------------------------------------
si = KVStore(mode="si")
s = si.begin()
s.set("x", 1)
s.set("y", 1)
s.commit()

t1 = si.begin()
t2 = si.begin()
t1.set("x", 0)
t1.commit()
print("t2 snapshot still sees x =", t2.get("x"))   # 1 (its snapshot)
t2.set("y", 0)
t2.commit()
print("write skew result:", si.dump(), "(x + y = 0: anomaly visible)")
