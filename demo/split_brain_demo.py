#!/usr/bin/env python3
"""Bonus 2 demo: network partitions must never produce two primaries (split brain).

Client threads write (and read) continuously and DIRECTLY to BOTH servers' client ports
- every attempt goes to a randomly chosen server, so a node that wrongly believed it is
primary would be caught answering. Then the network is partitioned and healed:

  phase 1  normal operation (A primary, lease from the witness, B in sync)
  phase 2  cut ONLY the A<->B link; both still reach the witness
           -> B must NOT promote (A holds the lease); A keeps serving, degraded
  phase 3  heal; A re-syncs B with a snapshot
  phase 4  isolate A from B AND from the witness (clients still reach A!)
           -> A fences itself (NO_QUORUM) within the lease TTL, only then B promotes
  phase 5  heal; A sees the higher epoch, steps down, resyncs as backup

Checks at the end (PASS/FAIL):
  1 no overlapping acks: every answer carries (served_by, epoch); no answer of an older
    epoch was sent after an answer of a newer epoch had been received (client clock),
    and - local mode - the server logs show the epochs' serving windows do not overlap
  2 final balance == initial + acknowledged deposits - acknowledged withdrawals
  3 each request_id acknowledged once (server logs: applied once; resend only replays)
  4 after healing the old primary stepped down and has identical data (digest) to B
  5 no acknowledged write lost (every acked id stored on the primary AND the backup)

Local mode (starts witness, A, B and TCP proxies that simulate the partitions):
    python3 demo/split_brain_demo.py --local
Three-VM mode (run on the client VM 192.168.75.20 next to the witness; you run the
iptables commands on A when the script asks):
    python3 demo/split_brain_demo.py --servers 192.168.75.10:5000,192.168.75.11:5000 \\
        --repl 192.168.75.10:6000,192.168.75.11:6000 --witness 127.0.0.1:7000
"""
import argparse
import json
import os
import random
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone

from _common import BankClient
from client import parse_server
from failover_demo import ROOT, LocalNode, free_port
from netproxy import TcpProxy

ACCOUNTS = ["A1001", "A1002", "A1003"]
RETRY = {"NOT_PRIMARY", "NO_QUORUM", "SERVER_BUSY"}


def ask(addr, msg, timeout=2.0):
    """One JSON request to a repl port or the witness; None if unreachable."""
    host, port = parse_server(addr)
    try:
        with socket.create_connection((host, port), timeout=timeout) as s:
            s.sendall((json.dumps(msg) + "\n").encode())
            buf = b""
            while not buf.endswith(b"\n"):
                chunk = s.recv(65536)
                if not chunk:
                    return None
                buf += chunk
            return json.loads(buf)
    except (OSError, ValueError):
        return None


# ------------------------------------------------------------------------ load
class Load:
    def __init__(self, servers, clients):
        self.servers = servers
        self.clients = clients
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.acks = []        # dicts: rid, kind, account, cents, status, code, node, epoch, sent, recv, replayed
        self.unknown = []
        self.refusals = {}    # (server, code) -> count

    def _attempt(self, conns, server, payload):
        conn = conns.get(server)
        try:
            if conn is None:
                host, port = parse_server(server)
                conn = conns[server] = BankClient(host, port, timeout=3)
            resp = conn.raw(payload)
            if resp is None:
                raise ConnectionError("closed")
            return resp
        except (OSError, ValueError) as exc:
            if conn is not None:
                conn.close()
            conns.pop(server, None)
            return {"code": f"NET:{type(exc).__name__}"}

    def worker(self, n):
        rnd, conns = random.Random(), {}
        while not self.stop.is_set():
            r = rnd.random()
            account = rnd.choice(ACCOUNTS)
            cents = rnd.randint(1, 5000)
            if r < 0.3:
                kind, req = "balance", {"action": "balance", "account": account}
            else:
                kind = "deposit" if r < 0.65 else "withdraw"
                req = {"action": kind, "account": account, "amount": cents / 100}
            rid = f"sb-{n}-{uuid.uuid4().hex[:10]}"
            req["request_id"] = rid
            payload = json.dumps(req).encode()
            started, attempt = time.monotonic(), 0
            while True:
                server = rnd.choice(self.servers)         # DIRECTLY to either server
                sent = time.monotonic()
                resp = self._attempt(conns, server, payload)
                recv = time.monotonic()
                code = resp.get("code")
                if code in RETRY or str(code).startswith("NET:"):
                    with self.lock:
                        key = (server, code)
                        self.refusals[key] = self.refusals.get(key, 0) + 1
                    attempt += 1
                    if recv - started > 40:
                        with self.lock:
                            self.unknown.append(req)
                        break
                    time.sleep(min(0.3, 0.02 * attempt))
                    continue
                with self.lock:
                    self.acks.append({"rid": rid, "kind": kind, "account": account, "cents": cents,
                                      "status": resp.get("status"), "code": code,
                                      "node": resp.get("served_by"), "epoch": resp.get("epoch"),
                                      "server": server, "sent": sent, "recv": recv,
                                      "replayed": resp.get("replayed", False)})
                break
            time.sleep(rnd.uniform(0, 0.03))
        for c in conns.values():
            c.close()


def primary_of(servers):
    """(server, status) of the node that answers balances (= the current primary)."""
    for _ in range(100):
        for s in servers:
            host, port = parse_server(s)
            try:
                with BankClient(host, port, timeout=3) as c:
                    resp = c.request(action="balance", account=ACCOUNTS[0])
                if resp.get("status") == "success":
                    return s
            except (OSError, ValueError):
                pass
        time.sleep(0.2)
    raise RuntimeError("no primary answers")


def balances_via(server):
    host, port = parse_server(server)
    with BankClient(host, port, timeout=5) as c:
        return {a: c.request(action="balance", account=a)["balance_cents"] for a in ACCOUNTS}


def resend(server, ack):
    host, port = parse_server(server)
    req = {"request_id": ack["rid"], "action": ack["kind"], "account": ack["account"]}
    if ack["kind"] != "balance":
        req["amount"] = ack["cents"] / 100
    with BankClient(host, port, timeout=5) as c:
        return c.request(**req)


# ---------------------------------------------------------------------- checks
def parse_ts(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def server_side_windows(logs):
    """From server logs: per epoch, (node set, first ack ts, last ack ts) of served requests."""
    win, applied = {}, {}
    for text in logs:
        for line in text.splitlines():
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            if ev.get("event") != "request" or ev.get("result") in RETRY or ev.get("epoch") is None:
                continue
            if ev.get("result") == "INTERNAL_ERROR":
                continue
            ts, e = parse_ts(ev["timestamp"]), ev["epoch"]
            nodes, first, last = win.get(e, (set(), ts, ts))
            nodes.add(ev["node_id"])
            win[e] = (nodes, min(first, ts), max(last, ts))
            if ev.get("result") == "success" and not ev.get("replayed") \
                    and ev.get("action") in ("deposit", "withdraw"):
                applied[ev["request_id"]] = applied.get(ev["request_id"], 0) + 1
    return win, applied


def verify(load, initial, servers, repl, logs=None, extra=None):
    print("\nresolving", len(load.unknown), "requests without an answer by resending the same id")
    primary = primary_of(servers)
    for req in load.unknown:
        server = primary_of(servers)
        host, port = parse_server(server)
        with BankClient(host, port, timeout=5) as c:
            resp = c.request(**req)
        t = time.monotonic()
        load.acks.append({"rid": req["request_id"], "kind": req["action"], "account": req["account"],
                          "cents": round(req.get("amount", 0) * 100), "status": resp.get("status"),
                          "code": resp.get("code"), "node": resp.get("served_by"),
                          "epoch": resp.get("epoch"), "server": server, "sent": t, "recv": t,
                          "replayed": resp.get("replayed", False)})
    acks = load.acks
    writes = [a for a in acks if a["kind"] != "balance" and a["status"] == "success"]
    expected = dict(initial)
    for a in writes:
        expected[a["account"]] += a["cents"] if a["kind"] == "deposit" else -a["cents"]

    checks = {}
    # (1) no overlapping acks
    by_epoch = {}
    for a in acks:
        by_epoch.setdefault(a["epoch"], []).append(a)
    epochs = sorted(e for e in by_epoch if e is not None)
    one_node = all(len({a["node"] for a in by_epoch[e]}) == 1 for e in epochs)
    order_ok, detail = True, []
    for i, e1 in enumerate(epochs):
        for e2 in epochs[i + 1:]:
            last_sent_old = max(a["sent"] for a in by_epoch[e1])
            first_recv_new = min(a["recv"] for a in by_epoch[e2])
            if last_sent_old > first_recv_new:
                order_ok = False
                detail.append(f"epoch {e1} answered a request sent {last_sent_old - first_recv_new:.3f}s "
                              f"after epoch {e2} had already answered")
    print("\nacks per epoch (client view):")
    for e in epochs:
        lst = by_epoch[e]
        print(f"  epoch {e}: node {sorted({a['node'] for a in lst})}  {len(lst)} answers  "
              f"first {min(a['recv'] for a in lst) - load.t0:6.2f}s  last {max(a['recv'] for a in lst) - load.t0:6.2f}s")
    if None in by_epoch:
        print(f"  (without epoch: {len(by_epoch[None])} answers - should be 0)")
    no_overlap = order_ok and one_node and None not in by_epoch
    if logs:
        win, applied = server_side_windows(logs)
        print("server logs (serving windows per epoch, wall clock):")
        ws = sorted(win.items())
        for e, (nodes, first, last) in ws:
            print(f"  epoch {e}: {sorted(nodes)} {datetime.fromtimestamp(first, timezone.utc).strftime('%H:%M:%S.%f')[:-3]}"
                  f" .. {datetime.fromtimestamp(last, timezone.utc).strftime('%H:%M:%S.%f')[:-3]} UTC")
        for (e1, w1), (e2, w2) in zip(ws, ws[1:]):
            gap = w2[1] - w1[2]
            print(f"  gap between last ack of epoch {e1} and first ack of epoch {e2}: {gap:.3f}s")
            no_overlap &= gap > 0 and len(w1[0]) == 1 and len(w2[0]) == 1
        dup = [r for r, n in applied.items() if n > 1]
        checks["3 each request_id applied once (server logs)"] = not dup
    for d in detail:
        print("  !!", d)
    checks["1 no overlapping acks of two primaries"] = no_overlap

    final = balances_via(primary)
    print("\nfinal balances via primary", primary, final, "expected", expected)
    checks["2 final balance == initial + acked operations"] = final == expected
    ids = [a["rid"] for a in acks]
    checks["3 each request_id acknowledged once (client)"] = len(ids) == len(set(ids))

    # (4) both nodes: roles and identical data
    time.sleep(1.0)
    st = [ask(r, {"type": "status"}) for r in repl]
    for r, s in zip(repl, st):
        print(f"  status {r}: " + (json.dumps({k: s[k] for k in ('node_id', 'role', 'epoch', 'lease',
                                                                    'synced', 'processed', 'digest')})
                                   if s else "unreachable"))
    roles_ok = st[0] and st[1] and sorted([st[0]["role"], st[1]["role"]]) == ["backup", "primary"]
    same = roles_ok and st[0]["digest"] == st[1]["digest"] and st[0]["balances"] == st[1]["balances"]
    if extra and "old_primary" in extra:
        old = next((s for s in st if s and s["node_id"] == extra["old_primary"]), None)
        roles_ok = roles_ok and old is not None and old["role"] == "backup"
    checks["4 old primary stepped down and resynced (identical digest)"] = bool(roles_ok and same)

    # (5) every acked write stored: resend -> replayed, balances unchanged, on both nodes
    not_replayed = sum(1 for a in writes if not resend(primary, a).get("replayed"))
    after = balances_via(primary)
    checks["5 no acknowledged write lost (resend only replays)"] = not_replayed == 0 and after == final

    if extra:
        checks.update(extra.get("checks", {}))
    ok = True
    print()
    for name, good in checks.items():
        ok &= bool(good)
        print(f"  [{'PASS' if good else 'FAIL'}] {name}")
    print(f"\nanswers: {len(acks)} ({len(writes)} successful writes); refused/failed attempts "
          f"by (server, code): {dict(sorted(load.refusals.items()))}")
    return ok


# ----------------------------------------------------------------------- local
def wait(pred, timeout, what):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.05)
    print(f"  (timeout waiting for {what})")
    return False


def local(args):
    workdir = tempfile.mkdtemp(prefix="bank-splitbrain-")
    wport, pa, pb, ra, rb = (free_port() for _ in range(5))
    wlog = os.path.join(workdir, "witness.log")
    witness = subprocess.Popen([sys.executable, "witness.py", "--port", str(wport), "--state",
                                os.path.join(workdir, "witness_state.json"), "--log-file", wlog],
                               cwd=ROOT, stdout=subprocess.DEVNULL)
    w_addr = f"127.0.0.1:{wport}"
    wait(lambda: ask(w_addr, {"type": "status"}) is not None, 10, "witness")
    p_ab = TcpProxy(("127.0.0.1", rb), name="A->B")      # A's replication link to B
    p_ba = TcpProxy(("127.0.0.1", ra), name="B->A")      # B's connections to A
    p_aw = TcpProxy(("127.0.0.1", wport), name="A->W")   # A's connections to the witness
    a = LocalNode("A", "primary", pa, ra, p_ab.port, pb, workdir)
    b = LocalNode("B", "backup", pb, rb, p_ba.port, pa, workdir)
    if not args.without_witness:
        a.args += ["--witness", p_aw.addr]
        b.args += ["--witness", w_addr]
    else:
        print("NEGATIVE CONTROL: nodes run WITHOUT the witness (bonus 1 behaviour) - the checks "
              "are expected to FAIL")
    a.start()
    b.start()
    procs = [a, b]
    servers = [f"127.0.0.1:{pa}", f"127.0.0.1:{pb}"]
    repl = [f"127.0.0.1:{ra}", f"127.0.0.1:{rb}"]
    print(f"witness {w_addr}, A {servers[0]} (repl {ra}), B {servers[1]} (repl {rb}); logs in {workdir}")
    ok = False
    try:
        b.wait_for('"snapshot_applied"')
        if not args.without_witness:
            a.wait_for('"lease_granted"')
            wait(lambda: (ask(w_addr, {"type": "status"}) or {}).get("in_sync"), 10, "witness in_sync")
        initial = balances_via(servers[0])
        print("initial balances", initial)
        load = Load(servers, args.clients)
        load.t0 = t0 = time.monotonic()
        threads = [threading.Thread(target=load.worker, args=(n,)) for n in range(args.clients)]
        for t in threads:
            t.start()

        def say(msg):
            print(f"\n>>> [{time.monotonic() - t0:5.1f}s] {msg}")

        def hold(seconds):
            time.sleep(seconds)
            with load.lock:
                n = len(load.acks)
            print(f"    ... {n} answers so far; witness: {ask(w_addr, {'type': 'status'})}")

        hold(args.phase)
        say("PHASE 2: cut the A<->B link only (both still reach the witness)")
        p_ab.cut(); p_ba.cut()
        hold(args.phase * 2)
        b_refused = '"promotion_refused"' in b.read_log() and '"promoted"' not in b.read_log()
        a_degraded = '"degraded_mode"' in a.read_log() and '"lease_lost"' not in a.read_log()
        print(f"    B refused promotion: {b_refused}; A still primary (degraded, lease held): {a_degraded}")
        say("PHASE 3: heal the link")
        n = a.read_log().count('"snapshot_sent"')
        p_ab.heal(); p_ba.heal()
        wait(lambda: a.read_log().count('"snapshot_sent"') > n, 15, "A re-syncs B")
        if not args.without_witness:
            wait(lambda: (ask(w_addr, {"type": "status"}) or {}).get("in_sync"), 10, "witness in_sync")
        hold(args.phase)
        say("PHASE 4: isolate A from B and from the witness (clients can still reach A)")
        cut_at = time.time()
        p_ab.cut(); p_ba.cut(); p_aw.cut()
        wait(lambda: '"promoted"' in b.read_log(), 20, "B promotion")
        hold(args.phase)
        say("PHASE 5: heal everything")
        p_ab.heal(); p_ba.heal(); p_aw.heal()
        wait(lambda: '"new_role": "backup"' in a.read_log(), 15, "A steps down")
        wait(lambda: '"snapshot_applied"' in a.read_log(), 15, "A resynced")
        hold(args.phase)
        load.stop.set()
        for t in threads:
            t.join()
        say("load stopped")

        def first_ts(text, event):
            for line in text.splitlines():
                if f'"event": "{event}"' in line:
                    ev = json.loads(line)
                    if parse_ts(ev["timestamp"]) >= cut_at - 0.5:
                        return parse_ts(ev["timestamp"])
            return None
        fenced, promoted = first_ts(a.read_log(), "lease_lost"), first_ts(b.read_log(), "promoted")
        if fenced and promoted:
            print(f"A fenced itself {fenced - cut_at:.2f}s after the cut; B promoted "
                  f"{promoted - cut_at:.2f}s after the cut ({promoted - fenced:.2f}s after A fenced)")
        for name, text in (("A", a.read_log()), ("B", b.read_log()), ("W", open(wlog).read())):
            for line in text.splitlines():
                if any(f'"event": "{k}"' in line for k in (
                        "lease_granted", "lease_lost", "lease_restored", "promoted", "role_change",
                        "promotion_refused", "degraded_mode", "sync_report", "lease_expired",
                        "snapshot_applied", "cannot_promote")):
                    ev = json.loads(line)
                    keep = {k: v for k, v in ev.items() if k not in ("client_ip", "note")}
                    print(f"  {name}: {json.dumps(keep)[:230]}")
        extra = {"old_primary": "A", "checks": {
            "B refused promotion while A held the lease (link-only partition)": b_refused,
            "A kept serving (degraded) during the link-only partition": a_degraded,
            "A fenced itself BEFORE B promoted (full isolation)":
                bool(fenced and promoted and fenced < promoted)}}
        ok = verify(load, initial, servers, repl, [a.read_log(), b.read_log()], extra)
    finally:
        for p in procs:
            p.stop()
        witness.terminate()
        witness.wait(10)
        for p in (p_ab, p_ba, p_aw):
            p.close()
    return ok


# -------------------------------------------------------------------------- VM
def vm(args):
    servers = args.servers.split(",")
    repl = args.repl.split(",") if args.repl else [f"{parse_server(s)[0]}:6000" for s in servers]
    st = ask(args.witness, {"type": "status"})
    print("witness:", st)
    if not st:
        sys.exit("witness not reachable - start it first (python3 witness.py)")
    primary = primary_of(servers)
    old = next((s for s in (ask(r, {"type": "status"}) for r in repl) if s and s["role"] == "primary"), None)
    initial = balances_via(primary)
    print("primary", primary, "initial balances", initial)
    load = Load(servers, args.clients)
    load.t0 = t0 = time.monotonic()
    threads = [threading.Thread(target=load.worker, args=(n,)) for n in range(args.clients)]
    for t in threads:
        t.start()

    def step(text):
        with load.lock:
            n = len(load.acks)
        print(f"\n[{time.monotonic() - t0:5.1f}s] {n} answers so far; witness: "
              f"{ask(args.witness, {'type': 'status'})}")
        input(f">>> {text}\n    press Enter when done ... ")

    step("On Server A run:  sudo bash demo/vm_partition.sh cut-link   (A<->B only), "
         "wait ~10 s")
    step("On Server A run:  sudo bash demo/vm_partition.sh heal        , wait ~5 s")
    step("On Server A run:  sudo bash demo/vm_partition.sh cut-all     (A<->B and A->witness), "
         "wait ~10 s (B should promote)")
    step("On Server A run:  sudo bash demo/vm_partition.sh heal        , wait ~5 s")
    load.stop.set()
    for t in threads:
        t.join()
    ok = verify(load, initial, servers, repl, None,
                {"old_primary": old["node_id"]} if old else None)
    return ok


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--local", action="store_true", help="run everything on this machine")
    ap.add_argument("--servers", help="client ports, e.g. 192.168.75.10:5000,192.168.75.11:5000")
    ap.add_argument("--repl", help="replication ports for status/digest (default HOST:6000)")
    ap.add_argument("--witness", default="127.0.0.1:7000")
    ap.add_argument("--clients", type=int, default=6)
    ap.add_argument("--phase", type=float, default=4.0, help="local: seconds per phase")
    ap.add_argument("--without-witness", action="store_true",
                    help="local negative control: run the SAME scenario with bonus 1 nodes "
                         "(no witness) to show that the checks do catch a split brain")
    args = ap.parse_args()
    if args.local:
        ok = local(args)
    elif args.servers:
        ok = vm(args)
    else:
        ap.error("use --local or --servers")
    print("\nOVERALL", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
