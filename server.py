#!/usr/bin/env python3
"""Mini Banking Transaction Server (Distributed Systems - Assignment 1).

Transport : TCP, one JSON object per line (see protocol.py).
Threads   : one thread per connected client (socketserver.ThreadingTCPServer),
            at most MAX_CLIENTS at the same time.
State     : SQLite database shared by all threads (see bank_db.py).

Usage: python3 server.py [--port 5000] [--max-clients 100] [--db bank.db] ...
Config precedence: command line > environment variables > config.json > defaults.
"""
import argparse
import json
import logging
import os
import signal
import socket
import socketserver
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

import bank_db
import protocol
from protocol import BankError

DEFAULTS = {
    "host": "0.0.0.0",
    "port": 5000,
    "max_clients": 100,
    "db_path": "bank.db",
    "log_file": "logs/server.log",
    "max_line_bytes": 65536,          # longer request lines are rejected
    "client_idle_timeout": 300,       # seconds; frees the slot of a silent client
    "enable_fault_injection": False,  # debug only!
}
# config key -> (environment variable, type)
ENV_VARS = {
    "host": ("BANK_HOST", str),
    "port": ("BANK_PORT", int),
    "max_clients": ("BANK_MAX_CLIENTS", int),
    "db_path": ("BANK_DB_PATH", str),
    "log_file": ("BANK_LOG_FILE", str),
    "max_line_bytes": ("BANK_MAX_LINE_BYTES", int),
    "client_idle_timeout": ("BANK_CLIENT_IDLE_TIMEOUT", float),
    "enable_fault_injection": ("BANK_ENABLE_FAULT_INJECTION", bool),
}

log = logging.getLogger("bank")


# ----------------------------------------------------------------------------- config
def _to_bool(value) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def load_config(argv=None) -> dict:
    ap = argparse.ArgumentParser(description="Mini Banking Transaction Server")
    ap.add_argument("--config", default="config.json", help="path to JSON config file")
    ap.add_argument("--host")
    ap.add_argument("--port", type=int)
    ap.add_argument("--max-clients", dest="max_clients", type=int)
    ap.add_argument("--db", dest="db_path")
    ap.add_argument("--log-file", dest="log_file")
    ap.add_argument("--enable-fault-injection", dest="enable_fault_injection",
                    action="store_const", const=True, help="DEBUG ONLY: honour simulate_failure")
    args = ap.parse_args(argv)

    cfg = dict(DEFAULTS)
    if os.path.exists(args.config):                       # 1) config file
        with open(args.config, encoding="utf-8") as f:
            cfg.update({k: v for k, v in json.load(f).items() if k in DEFAULTS})
    for key, (env, typ) in ENV_VARS.items():              # 2) environment
        if os.environ.get(env):
            cfg[key] = _to_bool(os.environ[env]) if typ is bool else typ(os.environ[env])
    for key in DEFAULTS:                                  # 3) command line
        if getattr(args, key, None) is not None:
            cfg[key] = getattr(args, key)

    cfg["enable_fault_injection"] = _to_bool(cfg["enable_fault_injection"])
    cfg["max_clients"] = int(cfg["max_clients"])
    if cfg["max_clients"] < 1:
        ap.error("max_clients must be >= 1")
    return cfg


# ---------------------------------------------------------------------------- logging
def utc_now() -> str:
    """ISO-8601 UTC timestamp with milliseconds, e.g. 2026-10-21T15:20:01.123Z"""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def setup_logging(log_file: str) -> None:
    if os.path.dirname(log_file):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, encoding="utf-8")):
        handler.setFormatter(logging.Formatter("%(message)s"))
        log.addHandler(handler)
    log.setLevel(logging.INFO)


def log_event(event: str, **fields) -> None:
    """Write one JSON line to stdout and the log file (logging is thread-safe)."""
    log.info(json.dumps({"timestamp": utc_now(), "event": event, **fields}, default=str))


# ----------------------------------------------------------------------------- server
def read_line(rfile, limit: int):
    """Read one request line. Returns (line, too_long); line is None on EOF."""
    line = rfile.readline(limit + 1)
    if not line:
        return None, False
    if len(line) > limit and not line.endswith(b"\n"):
        # Too long: throw away the rest of this line so the next line can be processed.
        while True:
            chunk = rfile.readline(limit)
            if not chunk or chunk.endswith(b"\n"):
                break
        return b"", True
    return line.rstrip(b"\r\n"), False


class ClientHandler(socketserver.BaseRequestHandler):
    """Runs in its own thread, serves one client connection until it disconnects."""

    def handle(self):
        srv, cfg = self.server, self.server.cfg
        self.ip, self.port = self.client_address[:2]
        self.request.settimeout(cfg["client_idle_timeout"])
        rfile = self.request.makefile("rb")
        bank = bank_db.Bank(cfg["db_path"], cfg["enable_fault_injection"])
        started, handled, reason = time.monotonic(), 0, "client_closed"
        log_event("connect", client_ip=self.ip, client_port=self.port,
                  active_clients=srv.active_count())
        try:
            while not srv.stopping.is_set():
                try:
                    line, too_long = read_line(rfile, cfg["max_line_bytes"])
                except TimeoutError:
                    reason = "idle_timeout"
                    break
                except OSError:                    # e.g. connection reset by peer
                    reason = "connection_error"
                    break
                if line is None:                   # EOF: client closed the connection
                    break
                if not too_long and not line.strip():
                    continue                       # ignore empty lines
                response = self.process(bank, line, too_long)
                try:
                    self.request.sendall((json.dumps(response) + "\n").encode("utf-8"))
                except OSError:
                    reason = "send_failed"         # client vanished; server keeps running
                    break
                handled += 1
        finally:
            if srv.stopping.is_set() and reason == "client_closed":
                reason = "server_shutdown"
            bank.close()
            rfile.close()
            log_event("disconnect", client_ip=self.ip, client_port=self.port, reason=reason,
                      requests=handled, duration_s=round(time.monotonic() - started, 3))

    def process(self, bank, line: bytes, too_long: bool) -> dict:
        """Parse, validate and execute one request; never raises."""
        t0 = time.perf_counter()
        request_id = "srv-" + uuid.uuid4().hex[:12]        # used if client sent none
        action = account = None
        try:
            if too_long:
                raise BankError("MALFORMED_REQUEST",
                                f"request line longer than {self.server.cfg['max_line_bytes']} bytes")
            obj = protocol.parse_line(line)
            request_id = protocol.request_id_of(obj, request_id)
            action, account = obj.get("action"), obj.get("account")   # for logging only
            req = protocol.validate(obj, request_id)
            response = bank.execute(req)
        except BankError as err:
            response = protocol.error_response(request_id, err,
                                               account if isinstance(account, str) else None)
        except Exception as exc:                            # bug, DB error, injected fault...
            log_event("internal_error", request_id=request_id, error=repr(exc))
            response = protocol.error_response(request_id, BankError(
                "INTERNAL_ERROR", "internal server error; the operation was rolled back"))
        log_event("request", request_id=request_id, client_ip=self.ip, client_port=self.port,
                  action=action if isinstance(action, str) else None,
                  account=account if isinstance(account, str) else None,
                  to_account=response.get("to_account"), amount=response.get("amount"),
                  result="success" if response["status"] == "success" else response["code"],
                  balance=response.get("balance"), replayed=response.get("replayed", False),
                  latency_ms=round((time.perf_counter() - t0) * 1000, 3))
        return response


class BankServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 128

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.lock = threading.Lock()
        self.active = set()             # sockets of currently served clients
        self.stopping = threading.Event()
        super().__init__((cfg["host"], cfg["port"]), ClientHandler)

    def active_count(self) -> int:
        with self.lock:
            return len(self.active)

    def process_request(self, sock, client_address):
        """Called for every accepted connection: enforce MAX_CLIENTS, then start a thread."""
        with self.lock:
            busy = len(self.active) >= self.cfg["max_clients"]
            if not busy:
                self.active.add(sock)
        if not busy:
            super().process_request(sock, client_address)   # starts the handler thread
            return
        log_event("rejected_busy", client_ip=client_address[0], client_port=client_address[1],
                  max_clients=self.cfg["max_clients"])
        try:
            sock.settimeout(2)
            msg = {"status": "error", "request_id": None, "code": "SERVER_BUSY",
                   "message": f"server is at its limit of {self.cfg['max_clients']} clients, try later"}
            sock.sendall((json.dumps(msg) + "\n").encode("utf-8"))
        except OSError:
            pass
        self.shutdown_request(sock)

    def process_request_thread(self, sock, client_address):
        try:
            super().process_request_thread(sock, client_address)
        finally:
            with self.lock:
                self.active.discard(sock)

    def stop(self, grace_seconds: float = 5.0):
        """Graceful shutdown: stop accepting, let in-flight requests finish, close clients."""
        self.stopping.set()
        self.shutdown()                 # stops serve_forever (no new connections)
        self.server_close()
        with self.lock:
            clients = list(self.active)
        for sock in clients:            # wakes up threads blocked in readline (EOF)
            try:
                sock.shutdown(socket.SHUT_RD)
            except OSError:
                pass
        deadline = time.monotonic() + grace_seconds
        while self.active_count() and time.monotonic() < deadline:
            time.sleep(0.05)


def main(argv=None):
    cfg = load_config(argv)
    setup_logging(cfg["log_file"])
    bank_db.init_db(cfg["db_path"])
    try:
        server = BankServer(cfg)
    except OSError as exc:
        log_event("startup_failed", error=str(exc), host=cfg["host"], port=cfg["port"])
        return 1

    stop_requested = threading.Event()

    def on_signal(signum, _frame):
        log_event("signal", signal=signal.Signals(signum).name)
        stop_requested.set()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.2}, daemon=True).start()
    log_event("server_started", host=cfg["host"], port=cfg["port"], max_clients=cfg["max_clients"],
              db_path=os.path.abspath(cfg["db_path"]),
              fault_injection=cfg["enable_fault_injection"], pid=os.getpid())
    if cfg["enable_fault_injection"]:
        log_event("warning", message="FAULT INJECTION ENABLED - debug use only")

    while not stop_requested.wait(0.5):
        pass
    log_event("server_stopping", active_clients=server.active_count())
    server.stop()
    log_event("server_stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
