#!/usr/bin/env python3
"""Race-condition demo: N clients withdraw X from the SAME account at the same moment.

Each client is a thread with its own TCP connection. A Barrier makes all threads send
their withdraw request at (almost) the same instant. A correct server must never
overdraw the account and the final balance must equal  initial - successes * X.

Example (fresh database, A1001 = 1000): 50 clients x 30 -> exactly 33 succeed, final 10.
  python3 demo/concurrent_withdrawals.py --host 192.168.75.10 --clients 50 --amount 30
"""
import argparse
import threading
import uuid
from collections import Counter

from _common import BankClient, add_server_args, require_host


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    add_server_args(ap)
    ap.add_argument("--account", default="A1001")
    ap.add_argument("--clients", type=int, default=50)
    ap.add_argument("--amount", type=int, default=30, help="whole currency units")
    args = ap.parse_args()
    require_host(ap, args)

    with BankClient(args.host, args.port) as c:
        initial = c.request(action="balance", account=args.account)["balance_cents"]
    print(f"Initial balance of {args.account}: {initial / 100:.2f}")

    barrier = threading.Barrier(args.clients)
    results = [None] * args.clients

    def worker(i):
        try:
            with BankClient(args.host, args.port) as c:      # separate connection per client
                busy = c.pending_message(0.2)
                barrier.wait()                              # everyone starts together
                results[i] = busy or c.request(request_id=f"race-{i}-{uuid.uuid4().hex[:8]}",
                                               action="withdraw", account=args.account,
                                               amount=args.amount)
        except Exception as exc:                            # barrier broken, network error...
            results[i] = {"status": "error", "code": f"CLIENT_EXCEPTION {exc!r}"}

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(args.clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    counts = Counter("success" if r["status"] == "success" else r["code"] for r in results)
    with BankClient(args.host, args.port) as c:
        final = c.request(action="balance", account=args.account)["balance_cents"]

    successes = counts["success"]
    expected_final = initial - successes * args.amount * 100
    expected_successes = min(args.clients, initial // (args.amount * 100))
    print(f"Clients: {args.clients}, amount each: {args.amount}")
    for key, n in sorted(counts.items()):
        print(f"  {key:<20} {n}")
    print(f"Final balance: {final / 100:.2f} (expected {expected_final / 100:.2f})")
    assert final == expected_final, "lost update detected!"
    assert final >= 0, "account overdrawn!"
    assert successes == expected_successes, f"expected {expected_successes} successful withdrawals"
    print("PASS: no lost updates, no overdraft, final == initial - successes * amount")


if __name__ == "__main__":
    main()
