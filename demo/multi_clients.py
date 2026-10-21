#!/usr/bin/env python3
"""Several clients run random mixed operations at the same time.

Every client has its own connection and does deposits, withdrawals, transfers and
balance checks on the shared accounts. At the end we check that no money was created
or destroyed:  total_after == total_before + deposits - withdrawals  (transfers move
money between accounts but never change the total).
  python3 demo/multi_clients.py --host 192.168.75.10 --clients 8 --ops 25
"""
import argparse
import random
import threading

from _common import BankClient, add_server_args, require_host

ACCOUNTS = ["A1001", "A1002", "A1003"]


def total(host, port):
    with BankClient(host, port) as c:
        return sum(c.request(action="balance", account=a)["balance_cents"] for a in ACCOUNTS)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    add_server_args(ap)
    ap.add_argument("--clients", type=int, default=8)
    ap.add_argument("--ops", type=int, default=25, help="operations per client")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    require_host(ap, args)

    before = total(args.host, args.port)
    lock = threading.Lock()
    stats = {"deposited": 0, "withdrawn": 0, "success": 0, "error": 0}

    def worker(n):
        rnd = random.Random(args.seed + n)
        with BankClient(args.host, args.port) as c:
            for i in range(args.ops):
                action = rnd.choice(["deposit", "withdraw", "transfer", "balance"])
                acc = rnd.choice(ACCOUNTS)
                req = {"request_id": f"mix-{n}-{i}-{rnd.random()}", "action": action, "account": acc}
                if action != "balance":
                    req["amount"] = rnd.randint(1, 30000) / 100      # 0.01 .. 300.00
                if action == "transfer":
                    req["to_account"] = rnd.choice([a for a in ACCOUNTS if a != acc])
                resp = c.request(**req)
                with lock:
                    stats[resp["status"]] += 1
                    if resp["status"] == "success" and action in ("deposit", "withdraw"):
                        key = "deposited" if action == "deposit" else "withdrawn"
                        stats[key] += round(req["amount"] * 100)
                print(f"client {n:>2} op {i:>2}: {action:<8} {acc} -> "
                      f"{resp['status']} {resp.get('code', '')} balance={resp.get('balance')}")

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(args.clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    after = total(args.host, args.port)
    expected = before + stats["deposited"] - stats["withdrawn"]
    print(f"\nrequests: {stats['success']} succeeded, {stats['error']} rejected")
    print(f"total money before {before / 100:.2f}, after {after / 100:.2f}, expected {expected / 100:.2f}")
    assert after == expected, "money was created or destroyed!"
    print("PASS: money is conserved")


if __name__ == "__main__":
    main()
