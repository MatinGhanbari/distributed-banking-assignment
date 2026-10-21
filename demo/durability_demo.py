#!/usr/bin/env python3
"""Durability demo: committed deposits survive a hard crash (kill -9) of the server.

Two modes:
  --local   : this script starts its own server (on this machine), deposits, kills the
              server with SIGKILL, restarts it and checks the balance. Good for a quick test.
              python3 demo/durability_demo.py --local
  (default) : two-VM mode. Deposits against a remote server, then asks YOU to run
              `kill -9 <server pid>` on the server VM and restart the server, then checks.
              python3 demo/durability_demo.py --host 192.168.75.10
"""
import argparse
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid

from _common import BankClient, add_server_args, require_host

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def deposit_many(host, port, account, count, amount):
    with BankClient(host, port) as c:
        before = c.request(action="balance", account=account)["balance_cents"]
        for i in range(count):
            r = c.request(request_id=f"dur-{uuid.uuid4().hex[:10]}", action="deposit",
                          account=account, amount=amount)
            assert r["status"] == "success", r
            print(f"  deposit #{i + 1}: success, balance now {r['balance']:.2f}")
    return before, before + count * amount * 100


def start_server(port, db, log):
    proc = subprocess.Popen([sys.executable, "server.py", "--port", str(port), "--db", db,
                             "--log-file", log], cwd=ROOT, stdout=subprocess.DEVNULL)
    for _ in range(100):                                   # wait until it accepts connections
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            return proc
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("server did not start")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    add_server_args(ap)
    ap.add_argument("--local", action="store_true", help="start/kill a local server automatically")
    ap.add_argument("--account", default="A1003")
    ap.add_argument("--count", type=int, default=5)
    ap.add_argument("--amount", type=int, default=100)
    args = ap.parse_args()

    if args.local:
        tmp = tempfile.mkdtemp(prefix="bank-durability-")
        db, log = os.path.join(tmp, "bank.db"), os.path.join(tmp, "server.log")
        with socket.socket() as s:                         # pick a free port
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        host = "127.0.0.1"
        proc = start_server(port, db, log)
        print(f"Started server pid {proc.pid} on port {port}, db {db}")
    else:
        require_host(ap, args)
        host, port = args.host, args.port

    before, expected = deposit_many(host, port, args.account, args.count, args.amount)
    print(f"Balance before: {before / 100:.2f}; after {args.count} committed deposits it must be "
          f"{expected / 100:.2f}")

    if args.local:
        os.kill(proc.pid, signal.SIGKILL)                  # hard crash: no cleanup at all
        proc.wait()
        print(f"Killed server with SIGKILL (exit code {proc.returncode}); restarting ...")
        proc = start_server(port, db, log)
    else:
        input("\nNow on the SERVER VM run:  kill -9 $(pgrep -f server.py)\n"
              "then restart it:            python3 server.py\n"
              "Press Enter here when the server is running again... ")

    with BankClient(host, port) as c:
        after = c.request(action="balance", account=args.account)["balance_cents"]
    print(f"Balance after crash + restart: {after / 100:.2f}")
    if args.local:
        proc.terminate()
        proc.wait()
    assert after == expected, "committed data was lost!"
    print("PASS: every acknowledged deposit survived the crash")


if __name__ == "__main__":
    main()
