#!/usr/bin/env python3
"""Idempotency demo: sending the same request (same request_id) twice applies it once.

This is what a client does when it did not receive the answer (e.g. network timeout)
and retries. python3 demo/idempotency_demo.py --host 192.168.75.10
"""
import argparse
import uuid

from _common import BankClient, add_server_args, require_host


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    add_server_args(ap)
    args = ap.parse_args()
    require_host(ap, args)

    rid = f"idem-{uuid.uuid4().hex[:10]}"
    with BankClient(args.host, args.port) as c:
        before = c.request(action="balance", account="A1002")["balance_cents"]
        first = c.request(request_id=rid, action="deposit", account="A1002", amount=25)
    with BankClient(args.host, args.port) as c:            # retry on a NEW connection
        second = c.request(request_id=rid, action="deposit", account="A1002", amount=25)
        conflict = c.request(request_id=rid, action="deposit", account="A1002", amount=99)
        after = c.request(action="balance", account="A1002")["balance_cents"]
    print(f"balance before          : {before / 100:.2f}")
    print(f"1st deposit 25 ({rid}): {first['status']}, balance {first['balance']:.2f}")
    print(f"2nd deposit 25 (same id) : {second['status']}, balance {second['balance']:.2f}, "
          f"replayed={second.get('replayed')}")
    print(f"same id, other amount    : {conflict['code']}")
    print(f"balance after            : {after / 100:.2f}")
    assert after == before + 2500 and second.get("replayed") is True
    print("PASS: deposit applied exactly once")


if __name__ == "__main__":
    main()
