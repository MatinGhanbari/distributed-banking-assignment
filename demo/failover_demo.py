#!/usr/bin/env python3
"""Bonus demo: clients keep working while the primary (Server A) crashes.

Several client threads continuously send random deposits/withdrawals (each with its
own request_id) to the pair A,B through FailoverClient. Kill A in the middle; B
takes over automatically. At the end the script checks, on the current primary:
  * final balance of every account == initial + acknowledged deposits - acknowledged withdrawals
  * every acknowledged request_id is stored exactly once (re-sending it is only "replayed")
  * no balance is negative
Three-VM mode (run on the client VM, kill A by hand while it runs):
  python3 demo/failover_demo.py --servers 192.168.75.10:5000,192.168.75.11:5000 --duration 40
Local mode (starts A and B on this machine, SIGKILLs A mid-run, restarts it as backup):
  python3 demo/failover_demo.py --local
"""
import argparse
import os
import random
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from _common import BankClient  # noqa: F401  (makes client.py importable)
from client import FailoverClient

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
ACCOUNTS = ["A1001", "A1002", "A1003"]


# ------------------------------------------------------------- local cluster helper
def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class LocalNode:
    """Runs one server.py of the pair on this machine (used by --local)."""

    def __init__(self, node_id, role, port, repl, peer_repl, peer_port, workdir):
        self.node_id, self.port = node_id, port
        self.db = os.path.join(workdir, f"{node_id.lower()}.db")
        self.log = os.path.join(workdir, f"{node_id.lower()}.log")
        self.args = [sys.executable, "server.py", "--node-id", node_id, "--initial-role", role,
                     "--port", str(port), "--repl-port", str(repl),
                     "--peer", f"127.0.0.1:{peer_repl}", "--peer-client", f"127.0.0.1:{peer_port}",
                     "--db", self.db, "--log-file", self.log]
        self.proc = None

    def read_log(self):
        if not os.path.exists(self.log):
            return ""
        with open(self.log, encoding="utf-8") as f:
            return f.read()

    def start(self):
        n = self.read_log().count('"server_started"')
        if self.proc is not None:
            self.proc.poll()                             # reap a previous (crashed) process
        self.proc = subprocess.Popen(self.args, cwd=ROOT, stdout=subprocess.DEVNULL)
        return n

    def wait_for(self, text, count=1, timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.read_log().count(text) >= count:
                return
            time.sleep(0.05)
        raise RuntimeError(f"{self.node_id}: timeout waiting for {text}")

    def balances(self):
        with sqlite3.connect(self.db) as conn:
            return dict(conn.execute("SELECT account_id, balance_cents FROM accounts"))

    def kill9(self):
        os.kill(self.proc.pid, signal.SIGKILL)
        self.proc.wait()

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(10)


def start_local_pair(workdir):
    pa, pb, ra, rb = (free_port() for _ in range(4))
    a = LocalNode("A", "primary", pa, ra, rb, pb, workdir)
    b = LocalNode("B", "backup", pb, rb, ra, pa, workdir)
    a.start()
    b.start()
    b.wait_for('"snapshot_applied"')                     # B is in sync with A
    a.wait_for('"snapshot_sent"')                        # A replicates to B
    return a, b


# ------------------------------------------------------------------------ the load
class Load:
    def __init__(self, servers, clients, quiet):
        self.servers, self.clients, self.quiet = servers, clients, quiet
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.acked = []          # (time, request_id, action, account, cents, status, server, replayed)
        self.unknown = []        # requests that never got an answer before the deadline
        self.retried = 0
        self.max_request_gap = 0.0
        self.per_server = {}

    def worker(self, n):
        rnd = random.Random()
        client = FailoverClient(self.servers, deadline=30, timeout=3,
                                on_retry=lambda srv, why, k: self.note_retry(n, srv, why, k))
        while not self.stop.is_set():
            action, account = rnd.choice(["deposit", "withdraw"]), rnd.choice(ACCOUNTS)
            cents = rnd.randint(1, 5000)                 # 0.01 .. 50.00
            rid = f"fo-{n}-{uuid.uuid4().hex[:10]}"
            req = {"request_id": rid, "action": action, "account": account, "amount": cents / 100}
            try:
                resp, info = client.request(**req)
            except TimeoutError:
                with self.lock:
                    self.unknown.append(req)
                continue
            with self.lock:
                self.acked.append((time.monotonic(), rid, action, account, cents, resp.get("status"),
                                   info["server"], resp.get("replayed", False), resp.get("code")))
                self.per_server[info["server"]] = self.per_server.get(info["server"], 0) + 1
                if info["attempts"] > 1:
                    self.retried += 1
                    self.max_request_gap = max(self.max_request_gap, info["elapsed"])
                    print(f"  client {n}: {rid} {action} {account} {cents / 100:.2f} answered by "
                          f"{info['server']} after {info['attempts']} attempts / {info['elapsed']:.2f}s"
                          f" -> {resp.get('status')}{' (replayed)' if resp.get('replayed') else ''}")
            time.sleep(rnd.uniform(0.0, 0.05))
        client.close()

    def note_retry(self, n, server, reason, attempt):
        if attempt == 1 or not self.quiet:
            print(f"  client {n}: {server} failed ({reason}) -> retrying same request on next server")

    def progress(self, start):
        while not self.stop.wait(2):
            with self.lock:
                done, per = len(self.acked), dict(self.per_server)
            print(f"[{time.monotonic() - start:5.1f}s] acknowledged {done:5d}  by server: {per}")


def run(args, servers, events=()):
    """Run the load for args.duration seconds; `events` = [(at_seconds, label, function)]."""
    check = FailoverClient(servers)
    initial = {a: check.request(action="balance", account=a)[0]["balance_cents"] for a in ACCOUNTS}
    print(f"initial balances: { {a: c / 100 for a, c in initial.items()} }")

    load = Load(servers, args.clients, quiet=True)
    start = time.monotonic()
    threads = [threading.Thread(target=load.worker, args=(n,)) for n in range(args.clients)]
    threads.append(threading.Thread(target=load.progress, args=(start,), daemon=True))
    for t in threads:
        t.start()
    for at, label, fn in sorted(events, key=lambda e: e[0]):
        time.sleep(max(0.0, at - (time.monotonic() - start)))
        print(f"\n>>> [{time.monotonic() - start:5.1f}s] {label}\n")
        fn()
    try:
        while time.monotonic() - start < args.duration:
            time.sleep(0.2)
    except KeyboardInterrupt:
        print("stopping early (Ctrl+C)")
    load.stop.set()
    for t in threads[:-1]:
        t.join()
    return verify(load, check, initial)


def verify(load, check, initial):
    # Requests whose outcome is unknown are simply sent again with the SAME id: the
    # server applies them once or replays the stored answer - so the outcome becomes known.
    for req in load.unknown:
        resp, info = check.request(**req)
        load.acked.append((time.monotonic(), req["request_id"], req["action"], req["account"],
                           round(req["amount"] * 100), resp.get("status"), info["server"],
                           resp.get("replayed", False), resp.get("code")))

    expected = dict(initial)
    ids = [a[1] for a in load.acked]
    successes = [a for a in load.acked if a[5] == "success"]
    for _, _, action, account, cents, *_ in successes:
        expected[account] += cents if action == "deposit" else -cents
    rejected = {}
    for a in load.acked:
        if a[5] != "success":
            rejected[a[8]] = rejected.get(a[8], 0) + 1

    final = {a: check.request(action="balance", account=a)[0]["balance_cents"] for a in ACCOUNTS}
    # Exactly-once check: every acknowledged request must already be stored on the
    # current primary -> re-sending it must be a pure replay that changes nothing.
    not_replayed = 0
    for _, rid, action, account, cents, *_ in successes:
        resp, _ = check.request(request_id=rid, action=action, account=account, amount=cents / 100)
        not_replayed += not resp.get("replayed")
    after_resend = {a: check.request(action="balance", account=a)[0]["balance_cents"] for a in ACCOUNTS}

    times = sorted(a[0] for a in load.acked)
    max_gap = max((b - a for a, b in zip(times, times[1:])), default=0.0)
    print("\n================ RESULT ================")
    print(f"acknowledged requests : {len(load.acked)} ({len(successes)} success, rejected {rejected})")
    print(f"answered by server    : {load.per_server}")
    print(f"needed retry/failover : {load.retried}   unknown at deadline (resolved by resend): "
          f"{len(load.unknown)}")
    replayed = sum(1 for a in successes if a[7])
    print(f"replayed answers      : {replayed} (applied + replicated before the crash, "
          "so the retry was NOT applied again)")
    print(f"max failover gap      : {max_gap:.2f}s with no acknowledgement at all; "
          f"slowest retried request {load.max_request_gap:.2f}s")
    ok = True
    for a in ACCOUNTS:
        good = final[a] == expected[a] == after_resend[a]
        ok &= good
        print(f"{a}: initial {initial[a] / 100:10.2f}  expected {expected[a] / 100:10.2f}  "
              f"final {final[a] / 100:10.2f}  {'OK' if good else 'MISMATCH'}")
    checks = {
        "final == initial + deposits - withdrawals": all(final[a] == expected[a] for a in ACCOUNTS),
        "each request_id acknowledged once": len(ids) == len(set(ids)),
        "every acked request stored (resend only replays)": not_replayed == 0
                                                             and after_resend == final,
        "no negative balances": all(v >= 0 for v in final.values()),
    }
    for name, good in checks.items():
        ok &= good
        print(f"  [{'PASS' if good else 'FAIL'}] {name}")
    print("PASS" if ok else "FAIL")
    return ok


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--servers", default=os.environ.get("BANK_SERVERS"),
                    help="HOST:PORT list, e.g. 192.168.75.10:5000,192.168.75.11:5000")
    ap.add_argument("--clients", type=int, default=6)
    ap.add_argument("--duration", type=float, default=None, help="seconds (default 40, local 15)")
    ap.add_argument("--local", action="store_true", help="start A+B here and kill A automatically")
    ap.add_argument("--no-restart", action="store_true", help="local: do not restart A as backup")
    args = ap.parse_args()

    if not args.local:
        if not args.servers:
            ap.error("--servers is required (or use --local)")
        args.duration = args.duration or 40
        print(f"Sending for {args.duration:.0f}s to {args.servers}. Kill Server A now and then "
              "(kill -9 or VM power off) and watch B take over.")
        sys.exit(0 if run(args, args.servers.split(",")) else 1)

    args.duration = args.duration or 15
    workdir = tempfile.mkdtemp(prefix="bank-failover-")
    a, b = start_local_pair(workdir)
    print(f"local pair: A=127.0.0.1:{a.port} (primary)  B=127.0.0.1:{b.port} (backup)  logs in {workdir}")
    kill_at = args.duration * random.uniform(0.25, 0.45)
    events = [(kill_at, "kill -9 Server A (primary) while clients are active", a.kill9)]
    if not args.no_restart:
        events.append((args.duration * 0.7, "restart Server A - it must rejoin as BACKUP", a.start))
    try:
        ok = run(args, [f"127.0.0.1:{a.port}", f"127.0.0.1:{b.port}"], events)
        if not args.no_restart:
            time.sleep(1.0)                              # let A finish its catch-up
            same = a.balances() == b.balances()
            print(f"  [{'PASS' if same else 'FAIL'}] restarted A rejoined as backup with identical "
                  f"balances: A={a.balances()} B={b.balances()}")
            ok &= same and '"new_role": "backup"' in a.read_log()
        for line in (a.read_log() + b.read_log()).splitlines():
            if any(k in line for k in ('"promoted"', '"role_change"', '"snapshot_applied"',
                                        '"degraded_mode"', '"primary_failure_detected"')):
                print("  log:", line)
    finally:
        a.stop()
        b.stop()
    print("OVERALL", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
