#!/usr/bin/env python3
"""Atomicity demo: the server crashes in the middle of a transfer -> nothing changes.

Needs a server started with fault injection (DEBUG ONLY):
  BANK_ENABLE_FAULT_INJECTION=1 python3 server.py      (or --enable-fault-injection)
then:  python3 demo/fault_injection_demo.py --host 192.168.75.10
The request carries "simulate_failure": true, so the server raises an exception right
after debiting the source account (before crediting the target). The transaction is
rolled back and both balances must be unchanged.
"""
import argparse

from _common import BankClient, add_server_args, require_host


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    add_server_args(ap)
    args = ap.parse_args()
    require_host(ap, args)

    with BankClient(args.host, args.port) as c:
        def bal(acc):
            return c.request(action="balance", account=acc)["balance_cents"]

        before = (bal("A1001"), bal("A1002"))
        print(f"before: A1001={before[0] / 100:.2f}  A1002={before[1] / 100:.2f}")
        for req in ({"action": "transfer", "account": "A1001", "to_account": "A1002", "amount": 50},
                    {"action": "withdraw", "account": "A1001", "amount": 50}):
            resp = c.request(simulate_failure=True, **req)
            print(f"{req['action']} with simulate_failure -> {resp.get('code', resp['status'])}: "
                  f"{resp.get('message', '')}")
        after = (bal("A1001"), bal("A1002"))
        print(f"after:  A1001={after[0] / 100:.2f}  A1002={after[1] / 100:.2f}")
    assert after == before, "partial update was stored! (is fault injection enabled?)"
    print("PASS: failed transaction was rolled back completely")


if __name__ == "__main__":
    main()
