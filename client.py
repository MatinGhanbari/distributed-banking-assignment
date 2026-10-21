#!/usr/bin/env python3
"""Client for the Mini Banking Transaction Server.

Interactive:  python3 client.py --host 192.168.75.10
One-shot:     python3 client.py --host 192.168.75.10 deposit A1001 100
              (prints the JSON response; exit code 0 = success, 1 = error)
Cluster:      python3 client.py --servers 192.168.75.10:5000,192.168.75.11:5000
              (bonus: automatic failover - retries the SAME request on the other server)

Commands: balance ACC | deposit ACC AMOUNT | withdraw ACC AMOUNT |
          transfer FROM TO AMOUNT | raw TEXT | rawhex HEX | help | quit
"""
import argparse
import json
import os
import re
import select
import shlex
import socket
import sys
import time
import uuid

JSON_NUMBER = re.compile(r"-?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?")
HELP = """  balance ACC                 show balance            e.g. balance A1001
  deposit ACC AMOUNT          add money               e.g. deposit A1001 100.50
  withdraw ACC AMOUNT         take money out          e.g. withdraw A1001 30
  transfer FROM TO AMOUNT     atomic transfer         e.g. transfer A1001 A1002 50
  raw TEXT                    send TEXT as one line   e.g. raw {"action":"balance"}
  rawhex HEX                  send arbitrary bytes    e.g. rawhex fffe00
  quit"""


class BankClient:
    """One TCP connection to the server. Also used by the demo scripts and tests."""

    def __init__(self, host: str, port: int, timeout: float = 30):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.rfile = self.sock.makefile("rb")

    def send_raw(self, data: bytes) -> None:
        """Send arbitrary bytes (a newline is appended if missing)."""
        if not data.endswith(b"\n"):
            data += b"\n"
        self.sock.sendall(data)

    def recv(self):
        """Read one response line; None if the server closed the connection."""
        line = self.rfile.readline()
        return json.loads(line) if line else None

    def raw(self, data: bytes):
        self.send_raw(data)
        return self.recv()

    def request(self, **fields):
        """Send a request built from keyword arguments and return the response dict."""
        return self.raw(json.dumps(fields).encode("utf-8"))

    def pending_message(self, wait: float = 0.3):
        """Return a message the server sent without being asked (e.g. SERVER_BUSY), else None."""
        ready, _, _ = select.select([self.sock], [], [], wait)
        if not ready:
            return None
        return self.recv() or {"status": "error", "code": "CONNECTION_CLOSED",
                               "message": "server closed the connection"}

    def close(self):
        try:
            self.rfile.close()
            self.sock.close()
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class FailoverClient:
    """Bonus: talks to a primary/backup pair and survives the primary's crash.

    Every request gets a request_id. If the connection breaks, times out, or the server
    answers NOT_PRIMARY / SERVER_BUSY / NO_QUORUM (bonus 2), the SAME bytes (same request_id) are sent to the next
    server, with a small backoff, until an answer arrives or `deadline` seconds pass.
    Because the servers remember processed request_ids (and replicate them), a retry can
    never apply a transaction twice: it is either applied once or answered with replayed=true.
    """
    RETRY_CODES = {"NOT_PRIMARY", "SERVER_BUSY", "NO_QUORUM"}

    def __init__(self, servers, deadline: float = 30.0, timeout: float = 5.0, on_retry=None):
        self.servers = [parse_server(s) for s in servers]
        self.deadline, self.timeout, self.on_retry = deadline, timeout, on_retry
        self.index = 0                  # server we currently talk to
        self.conn = None

    def request(self, **fields):
        """Returns (response, info) where info = {"server", "attempts", "elapsed"}."""
        fields.setdefault("request_id", f"cli-{uuid.uuid4().hex[:12]}")
        return self.call_raw(json.dumps(fields).encode("utf-8"))

    def call_raw(self, payload: bytes):
        start, attempts = time.monotonic(), 0
        while True:
            host, port = self.servers[self.index]
            attempts += 1
            try:
                if self.conn is None:
                    self.conn = BankClient(host, port, timeout=self.timeout)
                resp = self.conn.raw(payload)
                if resp is None:
                    raise ConnectionError("server closed the connection")
                if resp.get("code") not in self.RETRY_CODES:
                    return resp, {"server": f"{host}:{port}", "attempts": attempts,
                                  "elapsed": time.monotonic() - start}
                reason = resp["code"]
            except (OSError, ValueError) as exc:          # refused, reset, timeout, garbage
                reason = f"{type(exc).__name__}: {exc}"
            self.close()
            if time.monotonic() - start > self.deadline:
                raise TimeoutError(f"no server answered within {self.deadline}s (last: {reason})")
            if self.on_retry:
                self.on_retry(f"{host}:{port}", reason, attempts)
            self.index = (self.index + 1) % len(self.servers)
            # Try the other server at once; back off a little more after each full round.
            time.sleep(min(1.0, 0.1 * 2 ** ((attempts - 1) // len(self.servers))))

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None


def parse_server(text: str):
    host, _, port = text.strip().rpartition(":")
    return (host, int(port)) if host else (text.strip(), 5000)


def add_server_args(ap: argparse.ArgumentParser) -> None:
    """--host/--port options shared by client.py and the demo scripts."""
    ap.add_argument("--host", default=os.environ.get("BANK_SERVER_HOST"),
                    help="server IP address (default: $BANK_SERVER_HOST), e.g. 192.168.75.10")
    ap.add_argument("--port", type=int, default=int(os.environ.get("BANK_SERVER_PORT", 5000)))
    ap.add_argument("--servers", default=os.environ.get("BANK_SERVERS"),
                    help="bonus: comma-separated HOST:PORT list of the primary/backup pair, "
                         "e.g. 192.168.75.10:5000,192.168.75.11:5000 (default: $BANK_SERVERS)")


def require_host(ap: argparse.ArgumentParser, args) -> None:
    if not args.host and not getattr(args, "servers", None):
        ap.error("server host is required: use --host <server-ip> or set BANK_SERVER_HOST")


def build_payload(words, request_id=None, simulate_failure=False) -> bytes:
    """Turn a command like ['deposit', 'A1001', '100'] into a JSON request line.

    The amount is copied verbatim if it looks like a JSON number (so 10.005 is NOT
    rounded by the client - the server decides), otherwise it is sent as a string
    and the server answers INVALID_AMOUNT. Each request gets a unique request_id so
    that a retry of the same request is never applied twice (idempotency).
    """
    cmd, args = words[0].lower(), words[1:]
    shapes = {"balance": ["account"], "deposit": ["account", "amount"],
              "withdraw": ["account", "amount"], "transfer": ["account", "to_account", "amount"]}
    if cmd not in shapes:
        raise ValueError(f"unknown command '{cmd}' (type 'help')")
    if len(args) != len(shapes[cmd]):
        raise ValueError(f"usage: {cmd} " + " ".join(f.upper() for f in shapes[cmd]))
    fields = dict(zip(shapes[cmd], args))
    amount = fields.pop("amount", None)
    req = {"request_id": request_id or f"cli-{uuid.uuid4().hex[:12]}", "action": cmd, **fields}
    if simulate_failure:
        req["simulate_failure"] = True
    text = json.dumps(req)
    if amount is not None:
        amount_json = amount if JSON_NUMBER.fullmatch(amount) else json.dumps(amount)
        text = text[:-1] + f', "amount": {amount_json}}}'
    return text.encode("utf-8")


def pretty(resp) -> str:
    """One readable line for a response."""
    if resp is None:
        return "!! server closed the connection"
    if resp.get("status") == "success":
        line = f"OK   {resp['action']:<8} {resp['account']}  balance = {resp['balance_cents'] / 100:,.2f}"
        if "to_account" in resp:
            line += f"  |  {resp['to_account']} balance = {resp['to_balance_cents'] / 100:,.2f}"
        if resp.get("replayed"):
            line += "  (replayed: already processed, not applied again)"
        return line + f"   [id {resp['request_id']}]"
    extra = f" (balance {resp['balance']:,.2f})" if "balance" in resp else ""
    return f"ERR  {resp.get('code')}: {resp.get('message')}{extra}   [id {resp.get('request_id')}]"


def run_command(client: BankClient, line: str, verbose: bool):
    if line.startswith(("raw ", "rawhex ")):
        cmd, arg = line.split(" ", 1)
        data = bytes.fromhex(arg) if cmd == "rawhex" else arg.encode("utf-8")
        resp = client.raw(data)
    else:
        resp = client.raw(build_payload(shlex.split(line)))
    print(pretty(resp))
    if verbose:
        print("     " + json.dumps(resp))
    return resp


def main():
    ap = argparse.ArgumentParser(description="Mini Banking client")
    add_server_args(ap)
    ap.add_argument("--request-id", help="one-shot mode: use this request_id (idempotency)")
    ap.add_argument("--simulate-failure", action="store_true",
                    help="one-shot mode: ask the server to fail mid-transaction (needs fault injection)")
    ap.add_argument("-v", "--verbose", action="store_true", help="also print raw JSON responses")
    ap.add_argument("command", nargs="*", help="one-shot command, e.g. deposit A1001 100")
    args = ap.parse_args()
    require_host(ap, args)
    if args.servers:
        return failover_main(args)

    try:
        client = BankClient(args.host, args.port)
    except OSError as exc:
        sys.exit(f"cannot connect to {args.host}:{args.port}: {exc}")
    with client:
        busy = client.pending_message()
        if busy is not None:            # server refused us (MAX_CLIENTS reached)
            print(pretty(busy))
            sys.exit(1)

        if args.command:                # ---- one-shot mode
            try:
                resp = client.raw(build_payload(args.command, args.request_id, args.simulate_failure))
            except ValueError as exc:
                sys.exit(str(exc))
            print(json.dumps(resp))
            sys.exit(0 if resp and resp.get("status") == "success" else 1)

        print(f"Connected to {args.host}:{args.port}. Type 'help' for commands.")
        while True:                     # ---- interactive mode
            try:
                line = input("bank> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not line:
                continue
            if line in ("quit", "exit"):
                break
            if line == "help":
                print(HELP)
                continue
            try:
                if run_command(client, line, args.verbose) is None:
                    break
            except ValueError as exc:
                print(f"     {exc}")
            except OSError as exc:
                print(f"!! connection error: {exc}")
                break


def failover_main(args):
    """Client mode for the primary/backup cluster (bonus)."""
    def on_retry(server, reason, attempt):
        print(f"     retry #{attempt}: {server} -> {reason}; trying next server", file=sys.stderr)

    client = FailoverClient(args.servers.split(","), on_retry=on_retry)

    def send(words):
        resp, info = client.call_raw(build_payload(
            words, args.request_id if args.command else None, args.simulate_failure))
        return resp, info

    if args.command:
        try:
            resp, info = send(args.command)
        except (ValueError, TimeoutError) as exc:
            sys.exit(str(exc))
        print(json.dumps(resp))
        print(f"(answered by {info['server']} after {info['attempts']} attempt(s))", file=sys.stderr)
        sys.exit(0 if resp.get("status") == "success" else 1)

    print(f"Cluster mode: {args.servers}. Type 'help' for commands (raw/rawhex not available here).")
    while True:
        try:
            line = input("bank> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if line in ("quit", "exit"):
            break
        if line == "help":
            print(HELP)
            continue
        if not line:
            continue
        try:
            resp, info = send(shlex.split(line))
            print(pretty(resp) + f"   <- {info['server']}")
            if args.verbose:
                print("     " + json.dumps(resp))
        except (ValueError, TimeoutError) as exc:
            print(f"     {exc}")
    client.close()


if __name__ == "__main__":
    main()
