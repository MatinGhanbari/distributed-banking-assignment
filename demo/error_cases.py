#!/usr/bin/env python3
"""Sends every kind of bad request over ONE connection and prints the server's answers.

Shows that each error gets a meaningful JSON error and that the connection (and the
server) keep working afterwards.
  python3 demo/error_cases.py --host 192.168.75.10
"""
import argparse
import json
import os

from _common import BankClient, add_server_args, require_host

# (description, raw bytes to send, expected error code)
CASES = [
    ("1. unknown account", b'{"action":"balance","account":"Z9999"}', "UNKNOWN_ACCOUNT"),
    ("2. insufficient funds", b'{"action":"withdraw","account":"A1002","amount":999999}',
     "INSUFFICIENT_FUNDS"),
    ("3a. negative amount", b'{"action":"deposit","account":"A1001","amount":-50}', "INVALID_AMOUNT"),
    ("3b. zero amount", b'{"action":"deposit","account":"A1001","amount":0}', "INVALID_AMOUNT"),
    ("3c. text amount", b'{"action":"deposit","account":"A1001","amount":"abc"}', "INVALID_AMOUNT"),
    ("3d. boolean amount", b'{"action":"deposit","account":"A1001","amount":true}', "INVALID_AMOUNT"),
    ("3e. NaN amount", b'{"action":"deposit","account":"A1001","amount":NaN}', "INVALID_AMOUNT"),
    ("3f. Infinity amount", b'{"action":"deposit","account":"A1001","amount":Infinity}',
     "INVALID_AMOUNT"),
    ("3g. 3 decimals", b'{"action":"deposit","account":"A1001","amount":10.005}', "INVALID_AMOUNT"),
    ("4. unsupported action", b'{"action":"steal","account":"A1001","amount":10}',
     "UNSUPPORTED_ACTION"),
    ("5a. missing account", b'{"action":"withdraw","amount":10}', "MISSING_FIELD"),
    ("5b. missing amount", b'{"action":"deposit","account":"A1001"}', "MISSING_FIELD"),
    ("5c. missing action", b'{"account":"A1001"}', "MISSING_FIELD"),
    ("6a. broken JSON", b'{"action": "balance", "account": ', "MALFORMED_REQUEST"),
    ("6b. JSON but not an object", b'[1, 2, 3]', "MALFORMED_REQUEST"),
    ("6c. random bytes (not UTF-8)", b"\xff\xfe" + os.urandom(30).replace(b"\n", b"x"),
     "MALFORMED_REQUEST"),
    ("6d. line longer than 64 KB", b'{"pad":"' + b"x" * 70000 + b'"}', "MALFORMED_REQUEST"),
]


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    add_server_args(ap)
    args = ap.parse_args()
    require_host(ap, args)

    failures = 0
    with BankClient(args.host, args.port) as c:
        for name, payload, expected in CASES:
            resp = c.raw(payload)
            ok = resp is not None and resp.get("code") == expected
            failures += not ok
            print(f"[{'PASS' if ok else 'FAIL'}] {name:<30} -> {resp.get('code')}: {resp.get('message')}")
        # The same connection still works after all those bad requests:
        resp = c.request(action="balance", account="A1001")
        print(f"\nAfter all errors, same connection: balance A1001 -> {json.dumps(resp)}")
        failures += resp["status"] != "success"
    print("ALL PASS" if failures == 0 else f"{failures} FAILURE(S)")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
