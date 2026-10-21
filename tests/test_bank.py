"""Automated tests for the Mini Banking Transaction Server.

Run from the project directory:   python3 -m unittest -v tests/test_bank.py
Every test class starts its own server process on a free port with a fresh temporary
database, so the tests do not touch bank.db and do not depend on each other.
"""
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from collections import Counter
from decimal import Decimal

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import protocol                      # noqa: E402
import server as server_module       # noqa: E402
from client import BankClient        # noqa: E402


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ServerProcess:
    """Starts `server.py` as a real separate process (like on the server VM)."""

    def __init__(self, *extra_args, env=None, workdir=None):
        self.workdir = workdir or tempfile.mkdtemp(prefix="bank-test-")
        self.port = free_port()
        self.db = os.path.join(self.workdir, "bank.db")
        self.log = os.path.join(self.workdir, "server.log")
        self.args = ["--port", str(self.port), "--db", self.db, "--log-file", self.log, *extra_args]
        self.env = {**os.environ, **(env or {})}
        self.proc = None

    def _starts_logged(self):
        return self.read_log().count('"server_started"') if os.path.exists(self.log) else 0

    def start(self):
        already = self._starts_logged()
        self.proc = subprocess.Popen([sys.executable, "server.py", *self.args], cwd=ROOT,
                                     env=self.env, stdout=subprocess.DEVNULL)
        deadline = time.time() + 10
        while time.time() < deadline:   # ready when a NEW "server_started" line is logged
            if self._starts_logged() > already:
                return self
            time.sleep(0.05)
        raise RuntimeError("server did not start")

    def client(self):
        return BankClient("127.0.0.1", self.port, timeout=30)

    def balance(self, account):
        with self.client() as c:
            return c.request(action="balance", account=account)["balance_cents"]

    def read_log(self):
        with open(self.log, encoding="utf-8") as f:
            return f.read()

    def kill9(self):
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(10)


class ServerTestCase(unittest.TestCase):
    server_args = ("--max-clients", "60", "--enable-fault-injection")

    @classmethod
    def setUpClass(cls):
        cls.server = ServerProcess(*cls.server_args).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()


# --------------------------------------------------------------------------- unit tests
class TestAmountValidation(unittest.TestCase):
    def test_valid_amounts(self):
        for value, cents in [(1, 100), (Decimal("0.01"), 1), (Decimal("100.5"), 10050),
                             (Decimal("1E+2"), 10000), (Decimal("2.50"), 250)]:
            self.assertEqual(protocol.parse_amount_cents(value), cents)

    def test_invalid_amounts(self):
        for value in [-50, 0, "abc", "100", True, False, None, [], float("nan"), float("inf"),
                      Decimal("10.005"), Decimal("1E+999999"), 10**20]:
            with self.assertRaises(protocol.BankError) as ctx:
                protocol.parse_amount_cents(value)
            self.assertEqual(ctx.exception.code, "INVALID_AMOUNT", value)


class TestConfigPrecedence(unittest.TestCase):
    def test_cli_over_env_over_file(self):
        path = os.path.join(tempfile.mkdtemp(), "config.json")
        with open(path, "w") as f:
            json.dump({"max_clients": 5}, f)
        old = os.environ.pop("BANK_MAX_CLIENTS", None)
        try:
            self.assertEqual(server_module.load_config(["--config", path])["max_clients"], 5)
            os.environ["BANK_MAX_CLIENTS"] = "7"
            self.assertEqual(server_module.load_config(["--config", path])["max_clients"], 7)
            cfg = server_module.load_config(["--config", path, "--max-clients", "9"])
            self.assertEqual(cfg["max_clients"], 9)
        finally:
            os.environ.pop("BANK_MAX_CLIENTS", None)
            if old is not None:
                os.environ["BANK_MAX_CLIENTS"] = old


# -------------------------------------------------------------------- end-to-end tests
class TestBankServer(ServerTestCase):
    def test_concurrent_withdrawals(self):
        """50 clients x 30 from A1001 (1000): exactly 33 succeed, final balance 10."""
        n, amount = 50, 30
        barrier = threading.Barrier(n)
        results = []
        lock = threading.Lock()

        def worker(i):
            with self.server.client() as c:
                barrier.wait()
                r = c.request(request_id=f"w-{i}", action="withdraw", account="A1001", amount=amount)
            with lock:
                results.append(r)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        counts = Counter(r["status"] if r["status"] == "success" else r["code"] for r in results)
        self.assertEqual(counts["success"], 33)
        self.assertEqual(counts["INSUFFICIENT_FUNDS"], 17)
        self.assertEqual(self.server.balance("A1001"), 1000)          # 10.00 in cents

    def test_error_cases_keep_connection_alive(self):
        cases = [
            (b'{"action":"balance","account":"Z9999"}', "UNKNOWN_ACCOUNT"),
            (b'{"action":"withdraw","account":"A1002","amount":1000000}', "INSUFFICIENT_FUNDS"),
            (b'{"action":"deposit","account":"A1002","amount":-50}', "INVALID_AMOUNT"),
            (b'{"action":"deposit","account":"A1002","amount":0}', "INVALID_AMOUNT"),
            (b'{"action":"deposit","account":"A1002","amount":"abc"}', "INVALID_AMOUNT"),
            (b'{"action":"deposit","account":"A1002","amount":true}', "INVALID_AMOUNT"),
            (b'{"action":"deposit","account":"A1002","amount":NaN}', "INVALID_AMOUNT"),
            (b'{"action":"deposit","account":"A1002","amount":-Infinity}', "INVALID_AMOUNT"),
            (b'{"action":"deposit","account":"A1002","amount":1.001}', "INVALID_AMOUNT"),
            (b'{"action":"hack","account":"A1002"}', "UNSUPPORTED_ACTION"),
            (b'{"action":"deposit","amount":5}', "MISSING_FIELD"),
            (b'{"action":"transfer","account":"A1002","amount":5}', "MISSING_FIELD"),
            (b'{"action":"transfer","account":"A1002","to_account":"Z9999","amount":5}',
             "UNKNOWN_ACCOUNT"),
            (b'{"action":"transfer","account":"A1002","to_account":"A1002","amount":5}',
             "SAME_ACCOUNT"),
            (b'{"action": "balance", ', "MALFORMED_REQUEST"),
            (b'"just a string"', "MALFORMED_REQUEST"),
            (b"\xff\xfe\x00\x81garbage", "MALFORMED_REQUEST"),
            (b"[" * 50000, "MALFORMED_REQUEST"),
            (b"x" * 70000, "MALFORMED_REQUEST"),                         # > 64 KB line
        ]
        before = self.server.balance("A1002")
        with self.server.client() as c:
            for payload, code in cases:
                resp = c.raw(payload)
                self.assertEqual(resp["status"], "error", payload[:60])
                self.assertEqual(resp["code"], code, payload[:60])
                self.assertTrue(resp["message"])
            self.assertEqual(c.request(action="balance", account="A1002")["status"], "success")
        self.assertEqual(self.server.balance("A1002"), before)        # nothing changed

    def test_fault_injection_rolls_back(self):
        before = (self.server.balance("A1002"), self.server.balance("A1003"))
        with self.server.client() as c:
            for req in ({"action": "transfer", "account": "A1003", "to_account": "A1002"},
                        {"action": "withdraw", "account": "A1003"},
                        {"action": "deposit", "account": "A1003"}):
                resp = c.request(amount=10, simulate_failure=True, **req)
                self.assertEqual(resp["code"], "INTERNAL_ERROR")
        self.assertEqual((self.server.balance("A1002"), self.server.balance("A1003")), before)

    def test_idempotency(self):
        before = self.server.balance("A1003")
        with self.server.client() as c:
            first = c.request(request_id="same-id-1", action="deposit", account="A1003", amount=7.5)
            second = c.request(request_id="same-id-1", action="deposit", account="A1003", amount=7.5)
            other = c.request(request_id="same-id-1", action="withdraw", account="A1003", amount=1)
        self.assertEqual(first["status"], "success")
        self.assertTrue(second["replayed"])
        self.assertEqual(second["balance_cents"], first["balance_cents"])
        self.assertEqual(other["code"], "DUPLICATE_REQUEST_ID")
        self.assertEqual(self.server.balance("A1003"), before + 750)   # applied once

    def test_transfer_is_atomic_and_conserves_money(self):
        total = self.server.balance("A1002") + self.server.balance("A1003")
        with self.server.client() as c:
            resp = c.request(action="transfer", account="A1003", to_account="A1002", amount=12.34)
        self.assertEqual(resp["status"], "success")
        self.assertEqual(self.server.balance("A1002") + self.server.balance("A1003"), total)

    def test_client_disconnect_mid_request(self):
        s = socket.create_connection(("127.0.0.1", self.server.port))
        s.sendall(b'{"action": "dep')          # half a request, then vanish
        s.close()
        time.sleep(0.2)
        self.assertGreater(self.server.balance("A1002"), 0)          # server still serving


class TestFaultInjectionDisabled(ServerTestCase):
    server_args = ()

    def test_flag_is_refused_when_disabled(self):
        before = self.server.balance("A1001")
        with self.server.client() as c:
            resp = c.request(action="withdraw", account="A1001", amount=10, simulate_failure=True)
        self.assertEqual(resp["code"], "FAULT_INJECTION_DISABLED")
        self.assertEqual(self.server.balance("A1001"), before)


class TestMaxClients(ServerTestCase):
    server_args = ("--max-clients", "2")

    def test_limit(self):
        a, b = self.server.client(), self.server.client()
        self.assertIsNone(a.pending_message(0.3))
        self.assertIsNone(b.pending_message(0.3))
        with self.server.client() as c:
            busy = c.pending_message(1)
            self.assertEqual(busy["code"], "SERVER_BUSY")
            self.assertIsNone(c.recv())                               # and then closed
        a.close()
        time.sleep(0.3)
        with self.server.client() as d:                               # slot is free again
            self.assertIsNone(d.pending_message(0.3))
            self.assertEqual(d.request(action="balance", account="A1001")["status"], "success")
        b.close()
        self.assertIn('"rejected_busy"', self.server.read_log())


class TestDurability(unittest.TestCase):
    def test_kill_minus_9(self):
        srv = ServerProcess().start()
        try:
            with srv.client() as c:
                for _ in range(5):
                    self.assertEqual(c.request(action="deposit", account="A1002",
                                               amount=100)["status"], "success")
            srv.kill9()                                               # hard crash
            srv.start()                                               # same db file
            self.assertEqual(srv.balance("A1002"), 50_000 + 5 * 10_000)
        finally:
            srv.stop()


class TestGracefulShutdown(unittest.TestCase):
    def test_sigterm_with_connected_client(self):
        srv = ServerProcess().start()
        c = srv.client()
        c.request(action="balance", account="A1001")
        srv.proc.send_signal(signal.SIGTERM)
        self.assertEqual(srv.proc.wait(10), 0)
        self.assertIsNone(c.recv())                                   # connection closed cleanly
        c.close()
        log = srv.read_log()
        self.assertIn('"server_shutdown"', log)
        self.assertIn('"server_stopped"', log)


if __name__ == "__main__":
    unittest.main(verbosity=2)
