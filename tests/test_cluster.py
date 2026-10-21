"""Tests for the bonus primary/backup cluster (cluster.py).

Run from the project directory:   python3 -m unittest -v tests/test_cluster.py
Each test starts a fresh pair (A = primary, B = backup) as real processes on free ports.
Shorter timeouts than the defaults are used so the tests run quickly.
"""
import json
import os
import random
import sys
import tempfile
import threading
import time
import unittest
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "demo"))

from client import BankClient, FailoverClient   # noqa: E402
from failover_demo import LocalNode, free_port  # noqa: E402

FAST = ["--heartbeat-interval", "0.25", "--failover-timeout", "1.5"]


class ClusterTestCase(unittest.TestCase):
    extra_args = []

    def setUp(self):
        workdir = tempfile.mkdtemp(prefix="bank-cluster-test-")
        pa, pb, ra, rb = (free_port() for _ in range(4))
        self.a = LocalNode("A", "primary", pa, ra, rb, pb, workdir)
        self.b = LocalNode("B", "backup", pb, rb, ra, pa, workdir)
        for node in (self.a, self.b):
            node.args += FAST + self.extra_args
            node.start()
        self.b.wait_for('"snapshot_applied"')
        self.a.wait_for('"snapshot_sent"')                # replication link is active
        self.a.wait_for('"server_started"')
        self.servers = [f"127.0.0.1:{self.a.port}", f"127.0.0.1:{self.b.port}"]
        self.clients = []

    def tearDown(self):
        for c in self.clients:
            c.close()
        self.a.stop()
        self.b.stop()

    def client(self, **kw):
        client = FailoverClient(self.servers, deadline=20, timeout=3, **kw)
        self.clients.append(client)
        return client

    def balance(self, account):
        client = self.client()
        return client.request(action="balance", account=account)[0]["balance_cents"]


class TestCluster(ClusterTestCase):
    def test_backup_rejects_clients_with_not_primary(self):
        with BankClient("127.0.0.1", self.b.port) as c:
            resp = c.request(action="balance", account="A1001")
        self.assertEqual(resp["code"], "NOT_PRIMARY")
        self.assertEqual(resp["primary_hint"], f"127.0.0.1:{self.a.port}")

    def test_writes_are_on_backup_before_ack(self):
        resp, info = self.client().request(action="deposit", account="A1002", amount=12.5)
        self.assertEqual(info["server"], self.servers[0])
        self.assertEqual(self.b.balances()["A1002"], resp["balance_cents"])   # B already has it

    def test_failover_under_load_with_sigkill(self):
        stop, lock, acked = threading.Event(), threading.Lock(), []
        initial = {a: self.balance(a) for a in ("A1001", "A1002", "A1003")}

        def worker(n):
            rnd, client = random.Random(n), self.client()
            while not stop.is_set():
                req = {"request_id": f"t{n}-{uuid.uuid4().hex[:8]}", "action":
                       rnd.choice(["deposit", "withdraw"]), "account":
                       rnd.choice(list(initial)), "amount": rnd.randint(1, 3000) / 100}
                resp, info = client.request(**req)
                with lock:
                    acked.append((req, resp, info))
            client.close()

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(4)]
        for t in threads:
            t.start()
        time.sleep(1.5)
        self.a.kill9()                                   # crash the primary under load
        time.sleep(4)
        stop.set()
        for t in threads:
            t.join()

        expected = dict(initial)
        for req, resp, _ in acked:
            if resp["status"] == "success":
                cents = round(req["amount"] * 100)
                expected[req["account"]] += cents if req["action"] == "deposit" else -cents
        final = {a: self.balance(a) for a in initial}
        self.assertEqual(final, expected)
        self.assertTrue(all(v >= 0 for v in final.values()))
        ids = [req["request_id"] for req, _, _ in acked]
        self.assertEqual(len(ids), len(set(ids)))
        served_by = {info["server"] for _, _, info in acked}
        self.assertEqual(served_by, set(self.servers))   # A before the crash, B after
        self.assertIn('"promoted"', self.b.read_log())

    def test_replayed_after_failover(self):
        client = self.client()
        first, _ = client.request(request_id="dep-once", action="deposit", account="A1003", amount=40)
        self.a.kill9()
        again, info = client.request(request_id="dep-once", action="deposit", account="A1003", amount=40)
        self.assertEqual(info["server"], self.servers[1])
        self.assertTrue(again["replayed"])
        self.assertEqual(again["balance_cents"], first["balance_cents"])
        self.assertEqual(self.balance("A1003"), 250_000 + 4_000)               # applied once

    def test_old_primary_rejoins_as_backup_and_syncs(self):
        client = self.client()
        client.request(action="deposit", account="A1001", amount=1)
        self.a.kill9()
        client.request(action="deposit", account="A1001", amount=2)           # served by B
        self.a.start()                                   # A comes back with its old database
        self.a.wait_for('"snapshot_applied"')
        with BankClient("127.0.0.1", self.a.port) as c:
            self.assertEqual(c.request(action="balance", account="A1001")["code"], "NOT_PRIMARY")
        resp, info = client.request(action="deposit", account="A1001", amount=4)
        self.assertEqual(info["server"], self.servers[1])                     # B is still primary
        time.sleep(0.2)
        self.assertEqual(self.a.balances(), self.b.balances())                # A is a full copy
        self.assertEqual(self.a.balances()["A1001"], 100_000 + 700)
        log = self.a.read_log()
        self.assertIn('"new_role": "backup"', log)
        self.assertEqual(json.loads([l for l in log.splitlines() if '"server_started"' in l][-1])
                         ["role"], "backup")


class TestCrashAfterReplicate(ClusterTestCase):
    extra_args = ["--enable-fault-injection"]

    def test_half_done_request_is_not_lost_or_doubled(self):
        """A replicates to B, then dies BEFORE its own COMMIT and before replying."""
        client = self.client()
        resp, info = client.request(request_id="half-done", action="withdraw", account="A1002",
                                    amount=100, simulate_crash=True)
        # The client got no reply from A, retried on B, and B answered from the replicated copy.
        self.assertEqual(info["server"], self.servers[1])
        self.assertTrue(resp["replayed"])
        self.assertEqual(self.balance("A1002"), 50_000 - 10_000)                # applied once
        self.a.start()                                   # A's own copy never committed ...
        self.a.wait_for('"snapshot_applied"')
        time.sleep(0.2)
        self.assertEqual(self.a.balances()["A1002"], 40_000)                   # ... resynced from B


if __name__ == "__main__":
    unittest.main(verbosity=2)
