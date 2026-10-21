"""Tests for bonus 2: witness, leases, fencing (witness.py + cluster.py in witness mode).

Run from the project directory:   python3 -m unittest -v tests/test_witness.py
Unit tests drive WitnessState with a fake clock; the process tests start a real witness,
A and B (shortened lease/heartbeat times) and TCP proxies to cut the network.
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "demo"))

from client import BankClient                     # noqa: E402
from failover_demo import LocalNode, free_port    # noqa: E402
from netproxy import TcpProxy                     # noqa: E402
from witness import WitnessState                  # noqa: E402

FAST = ["--heartbeat-interval", "0.25", "--failover-timeout", "1.5", "--lease-ttl", "2.0",
        "--lease-renew", "0.4", "--lease-margin", "0.3", "--witness-timeout", "0.5"]


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class TestWitnessState(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(tempfile.mkdtemp(prefix="witness-unit-"), "w.json")
        self.clock = Clock()
        self.w = WitnessState(self.path, grace=1.0, clock=self.clock)

    def test_first_acquire_gets_epoch_1_and_is_persisted(self):
        reply, event = self.w.acquire("A", 0, 3.0)
        self.assertEqual((reply["ok"], reply["granted"], reply["epoch"], reply["holder"]),
                         (True, True, 1, "A"))
        self.assertEqual(event["event"], "lease_granted")
        with open(self.path) as f:
            self.assertEqual(json.load(f)["holder"], "A")

    def test_cas_refuses_while_lease_held_and_during_grace(self):
        self.w.acquire("A", 0, 3.0)
        self.w.acquire("A", 1, 3.0, True, 1)             # A reports: B is in sync
        self.clock.t += 2.0
        self.assertFalse(self.w.acquire("B", 1, 3.0)[0]["ok"])      # lease still running
        self.clock.t += 1.5                                         # expired, but in grace
        self.assertFalse(self.w.acquire("B", 1, 3.0)[0]["ok"])
        self.clock.t += 1.0                                         # past expiry + grace
        reply, _ = self.w.acquire("B", 1, 3.0)
        self.assertEqual((reply["ok"], reply["holder"], reply["epoch"]), (True, "B", 2))
        # Only one winner: A (old holder) is now refused and learns the new holder/epoch.
        reply, _ = self.w.acquire("A", 1, 3.0)
        self.assertEqual((reply["ok"], reply["holder"], reply["epoch"]), (False, "B", 2))

    def test_renewal_keeps_the_lease(self):
        self.w.acquire("A", 0, 3.0)
        self.w.acquire("A", 1, 3.0, True, 1)
        for _ in range(10):
            self.clock.t += 1.0
            self.assertTrue(self.w.acquire("A", 1, 3.0, True, 1)[0]["ok"])
            self.assertFalse(self.w.acquire("B", 1, 3.0)[0]["ok"])

    def test_no_promotion_if_backup_not_in_sync(self):
        self.w.acquire("A", 0, 3.0)
        self.w.acquire("A", 1, 3.0, True, 1)
        self.w.acquire("A", 1, 3.0, False, 2)            # A: link down, B misses writes
        self.clock.t += 10
        reply, _ = self.w.acquire("B", 1, 3.0)
        self.assertFalse(reply["ok"])
        self.assertIn("not in sync", reply["reason"])
        self.assertTrue(self.w.acquire("A", 1, 3.0)[0]["ok"])      # A may come back

    def test_older_sync_reports_are_ignored(self):
        self.w.acquire("A", 0, 3.0)
        self.w.acquire("A", 1, 3.0, False, 5)
        self.w.acquire("A", 1, 3.0, True, 4)             # late, reordered report
        self.assertFalse(self.w.status()["in_sync"])

    def test_stale_epoch_is_refused(self):
        self.w.acquire("A", 0, 3.0)
        self.w.acquire("A", 1, 3.0, True, 1)
        self.clock.t += 10
        self.w.acquire("B", 1, 3.0)                      # B -> epoch 2
        self.w.acquire("B", 2, 3.0, True, 1)
        self.clock.t += 10
        reply, _ = self.w.acquire("C", 1, 3.0)           # someone who missed epoch 2
        self.assertFalse(reply["ok"])
        self.assertIn("stale epoch", reply["reason"])

    def test_restart_keeps_state_and_holds_lease_one_ttl(self):
        self.w.acquire("A", 0, 3.0)
        self.w.acquire("A", 1, 3.0, True, 1)
        self.clock.t += 100                              # long ago... then the witness restarts
        w2 = WitnessState(self.path, grace=1.0, clock=self.clock)
        self.assertEqual((w2.epoch, w2.holder, w2.in_sync), (1, "A", True))
        self.assertFalse(w2.acquire("B", 1, 3.0)[0]["ok"])   # conservatively still held
        self.clock.t += 4.1
        self.assertTrue(w2.acquire("B", 1, 3.0)[0]["ok"])


def ask(port, msg):
    with socket.create_connection(("127.0.0.1", port), timeout=3) as s:
        s.sendall((json.dumps(msg) + "\n").encode())
        return json.loads(s.makefile().readline())


class WitnessClusterCase(unittest.TestCase):
    def setUp(self):
        self.workdir = tempfile.mkdtemp(prefix="bank-witness-test-")
        self.wport, pa, pb, ra, rb = (free_port() for _ in range(5))
        self.ra, self.rb = ra, rb
        self.wlog = os.path.join(self.workdir, "witness.log")
        self.witness = None
        self.start_witness()
        self.p_ab = TcpProxy(("127.0.0.1", rb))
        self.p_ba = TcpProxy(("127.0.0.1", ra))
        self.a = LocalNode("A", "primary", pa, ra, self.p_ab.port, pb, self.workdir)
        self.b = LocalNode("B", "backup", pb, rb, self.p_ba.port, pa, self.workdir)
        for node in (self.a, self.b):
            node.args += FAST + ["--witness", f"127.0.0.1:{self.wport}"]
            node.start()
        self.a.wait_for('"lease_granted"')
        self.b.wait_for('"snapshot_applied"')
        self.wait(lambda: ask(self.wport, {"type": "status"})["in_sync"], 10)

    def start_witness(self):
        self.witness = subprocess.Popen(
            [sys.executable, "witness.py", "--port", str(self.wport), "--grace", "0.5",
             "--state", os.path.join(self.workdir, "w.json"), "--log-file", self.wlog],
            cwd=ROOT, stdout=subprocess.DEVNULL)
        self.wait(lambda: self._witness_up(), 10)

    def _witness_up(self):
        try:
            ask(self.wport, {"type": "status"})
            return True
        except OSError:
            return False

    def tearDown(self):
        self.a.stop()
        self.b.stop()
        if self.witness.poll() is None:
            self.witness.terminate()
            self.witness.wait(10)
        self.p_ab.close()
        self.p_ba.close()

    @staticmethod
    def wait(pred, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if pred():
                return True
            time.sleep(0.05)
        raise AssertionError("condition not reached in time")

    @staticmethod
    def call(node, **req):
        req.setdefault("request_id", f"t-{uuid.uuid4().hex[:10]}")
        with BankClient("127.0.0.1", node.port, timeout=5) as c:
            return c.request(**req)


class TestWitnessCluster(WitnessClusterCase):
    def test_lease_expiry_fences_primary_when_witness_down(self):
        """Witness gone: A stops serving within the lease TTL (CP), B does not promote."""
        self.assertEqual(self.call(self.a, action="deposit", account="A1001", amount=1)["status"],
                         "success")
        self.witness.kill()
        self.witness.wait()
        t0 = time.monotonic()
        self.wait(lambda: self.call(self.a, action="balance", account="A1001").get("code")
                  == "NO_QUORUM", 5)
        self.assertLess(time.monotonic() - t0, 2.0 + 0.5)      # within the TTL (+ slack)
        self.wait(lambda: '"lease_lost"' in self.a.read_log(), 2)   # logged at the deadline
        resp = self.call(self.a, action="deposit", account="A1001", amount=1)
        self.assertEqual(resp["code"], "NO_QUORUM")              # writes refused too
        time.sleep(2.5)                                          # > failover timeout
        self.assertNotIn('"promoted"', self.b.read_log())
        self.assertEqual(self.call(self.b, action="balance", account="A1001")["code"],
                         "NOT_PRIMARY")
        # witness back -> A renews its own lease and serves again (same epoch)
        self.start_witness()
        self.wait(lambda: self.call(self.a, action="balance", account="A1001")
                  .get("status") == "success", 10)
        self.assertIn('"lease_restored"', self.a.read_log())

    def test_no_promotion_while_lease_held(self):
        """A<->B cut, both reach the witness: B is refused, A keeps serving (degraded)."""
        self.p_ab.cut()
        self.p_ba.cut()
        time.sleep(4.0)                                          # >> failover timeout + ttl
        self.assertIn('"promotion_refused"', self.b.read_log())
        self.assertNotIn('"promoted"', self.b.read_log())
        resp = self.call(self.a, action="deposit", account="A1002", amount=2.5)
        self.assertEqual(resp["status"], "success")
        self.assertEqual((resp["served_by"], resp["epoch"]), ("A", 1))
        self.assertEqual(self.call(self.b, action="balance", account="A1002")["code"],
                         "NOT_PRIMARY")
        # the witness knows B misses writes -> B could not take over even after A dies
        self.assertFalse(ask(self.wport, {"type": "status"})["in_sync"])
        self.p_ab.heal()
        self.p_ba.heal()
        self.wait(lambda: ask(self.wport, {"type": "status"})["in_sync"], 10)
        self.assertEqual(self.a.balances(), self.b.balances())

    def test_isolated_primary_fences_then_backup_promotes_and_stale_epoch_rejected(self):
        p_aw = TcpProxy(("127.0.0.1", self.wport))
        try:
            self.a.args[self.a.args.index("--witness") + 1] = p_aw.addr
            self.a.stop()
            self.a.start()                                       # A now reaches W via proxy
            self.a.wait_for('"lease_granted"', 1)                # restarted holder, same lease
            self.wait(lambda: self.a.read_log().count('"snapshot_sent"') >= 2, 10)
            self.wait(lambda: ask(self.wport, {"type": "status"})["in_sync"], 10)
            self.p_ab.cut(); self.p_ba.cut(); p_aw.cut()
            self.b.wait_for('"promoted"', timeout=15)
            self.assertIn('"lease_lost"', self.a.read_log())
            self.assertEqual(self.call(self.a, action="balance", account="A1001")["code"],
                             "NO_QUORUM")
            resp = self.call(self.b, action="deposit", account="A1001", amount=1)
            self.assertEqual((resp["status"], resp["served_by"], resp["epoch"]), ("success", "B", 2))
            # A record from the old epoch is refused by the new primary (epoch fencing).
            stale = {"type": "record", "node_id": "A", "epoch": 1, "lsn": 10 ** 6,
                     "request_id": "x", "balances": {"A1001": 1}, "processed": None}
            self.assertEqual(ask(self.rb, stale)["error"], "STALE_EPOCH")
            hb = {"type": "heartbeat", "node_id": "A", "epoch": 1, "lsn": 0, "lease": True}
            self.assertEqual(ask(self.rb, hb)["error"], "STALE_EPOCH")
            self.p_ab.heal(); self.p_ba.heal(); p_aw.heal()
            self.a.wait_for('"new_role": "backup"', timeout=10)
            self.wait(lambda: self.a.balances() == self.b.balances(), 10)
        finally:
            p_aw.close()

    def test_lone_restart_does_not_become_primary(self):
        """A crashes, B takes over, B dies too; A restarted alone must NOT serve."""
        self.a.kill9()
        self.b.wait_for('"promoted"', timeout=15)
        self.call(self.b, action="deposit", account="A1003", amount=7)
        self.b.stop()
        self.a.start()                                           # persisted role: primary
        self.a.wait_for('"startup_witness"')
        self.a.wait_for('"server_started"', 2)
        time.sleep(1.5)
        resp = self.call(self.a, action="balance", account="A1003")
        self.assertEqual(resp["code"], "NOT_PRIMARY")
        self.assertIn("witness says B is primary", self.a.read_log())
        self.assertEqual(self.a.read_log().count('"lease_granted"'), 1)   # only the first

    def test_restart_without_witness_does_not_serve(self):
        self.a.stop()
        self.witness.kill()
        self.witness.wait()
        self.a.start()
        self.a.wait_for('"startup_witness"')
        self.a.wait_for('"server_started"', 2)
        resp = self.call(self.a, action="balance", account="A1001")
        self.assertEqual(resp["code"], "NO_QUORUM")


if __name__ == "__main__":
    unittest.main()
