#!/usr/bin/env python3
"""MAX_CLIENTS demo: open more connections than the server allows.

Start the server with a small limit, e.g.  python3 server.py --max-clients 3
then:  python3 demo/max_clients_demo.py --host 192.168.75.10 --limit 3
The first LIMIT connections are served, the next one gets SERVER_BUSY and is closed.
"""
import argparse
import time

from _common import BankClient, add_server_args, require_host


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    add_server_args(ap)
    ap.add_argument("--limit", type=int, default=3, help="the server's max_clients value")
    args = ap.parse_args()
    require_host(ap, args)

    clients = []
    for i in range(args.limit + 1):
        c = BankClient(args.host, args.port)
        clients.append(c)
        busy = c.pending_message(0.5)
        if busy:
            print(f"connection {i + 1}: REJECTED -> {busy['code']}: {busy['message']}")
        else:
            r = c.request(action="balance", account="A1001")
            print(f"connection {i + 1}: accepted -> balance A1001 = {r['balance']:.2f}")

    clients.pop(0).close()                                 # free one slot ...
    time.sleep(0.3)
    with BankClient(args.host, args.port) as c:            # ... and a new client gets in again
        busy = c.pending_message(0.5)
        print("after closing one connection, a new client is",
              "REJECTED" if busy else "accepted")
    for c in clients:
        c.close()


if __name__ == "__main__":
    main()
