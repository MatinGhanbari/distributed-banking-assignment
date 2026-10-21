#!/usr/bin/env python3
"""Witness (arbiter) for split-brain prevention - Advanced Bonus 2.

Runs on a THIRD machine (the client VM 192.168.75.20, port 7000). It is the single
authority that decides which bank server may be primary:

  * it hands out a LEASE (time-limited permission to serve) to one node at a time,
  * every new primary gets a new, strictly higher EPOCH,
  * promotion is an atomic compare-and-set: a node gets the lease only if the current
    lease has expired (+ a grace period) and the old primary reported that the other
    node is in sync (otherwise acknowledged writes could be lost),
  * the state (epoch, holder, in_sync) is written to disk with fsync BEFORE answering.

The witness stores no bank data; it is tiny and cheap, but with the two servers it forms a
majority of three ("2 of 3 votes": a node + the witness).

Protocol (one JSON object per line, one reply per request):
  {"type": "acquire", "node_id": "A", "epoch": 3, "ttl": 3.0, "in_sync": true, "sync_seq": 7}
      -> {"ok": true,  "epoch": 3, "holder": "A", "granted": false}    (lease renewed)
      -> {"ok": true,  "epoch": 4, "holder": "B", "granted": true}     (new primary, new epoch)
      -> {"ok": false, "reason": "...", "epoch": 3, "holder": "A"}     (refused)
  {"type": "status"} -> {"epoch": 3, "holder": "A", "lease_remaining": 2.1, "in_sync": true}

Usage: python3 witness.py [--port 7000] [--state witness_state.json] [--grace 1.0]
       python3 witness.py --status 192.168.75.20:7000      (print the current state)
"""
import argparse
import json
import os
import signal
import socketserver
import sys
import threading
import time

from server import log_event, setup_logging


class WitnessState:
    """The arbiter's decision logic (no networking - easy to unit test)."""

    def __init__(self, path: str, grace: float = 1.0, clock=time.monotonic):
        self.path, self.grace, self.clock = path, grace, clock
        self.lock = threading.Lock()
        data = {"epoch": 0, "holder": None, "in_sync": False, "sync_seq": 0, "ttl": 0.0}
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                data.update(json.load(f))
        self.epoch, self.holder = data["epoch"], data["holder"]
        self.in_sync, self.sync_seq, self.ttl = data["in_sync"], data["sync_seq"], data["ttl"]
        # We do not know when the last lease was granted before a restart, so we assume
        # the worst: it was granted just now. Nobody else can take over for one TTL.
        self.expiry = self.clock() + self.ttl if self.holder else 0.0
        self.expired_logged = False

    def _persist(self):
        """Write state atomically and durably: temp file + fsync + rename + fsync dir."""
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"epoch": self.epoch, "holder": self.holder, "in_sync": self.in_sync,
                       "sync_seq": self.sync_seq, "ttl": self.ttl}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)
        dir_fd = os.open(os.path.dirname(os.path.abspath(self.path)), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def acquire(self, node: str, epoch: int, ttl: float, in_sync=None, sync_seq=None):
        """Renew (current holder) or compare-and-set promotion (anyone else).

        Returns (reply, event) - event is a dict to log, or None.
        """
        now = self.clock()
        with self.lock:
            if node == self.holder:                              # ---- renew
                self.expiry, self.expired_logged = now + ttl, False
                event = None
                if sync_seq is not None and sync_seq > self.sync_seq:
                    changed = in_sync != self.in_sync
                    self.in_sync, self.sync_seq = bool(in_sync), sync_seq
                    if changed:
                        self._persist()                          # only real changes hit the disk
                        event = {"event": "sync_report", "holder": node, "epoch": self.epoch,
                                 "in_sync": self.in_sync, "sync_seq": sync_seq}
                return {"ok": True, "epoch": self.epoch, "holder": node, "granted": False}, event

            if self.holder is not None:                          # ---- promotion request
                wait = self.expiry + self.grace - now
                if wait > 0:
                    return self._refuse(f"lease held by {self.holder} (free in {wait:.1f}s)", wait)
                if not self.in_sync:
                    return self._refuse(f"{self.holder} reported that the other node may miss "
                                        "acknowledged writes (not in sync)")
            if epoch < self.epoch:
                return self._refuse(f"stale epoch {epoch} < {self.epoch}: you missed a newer primary")
            old = self.holder
            self.epoch = max(self.epoch, epoch) + 1
            self.holder, self.in_sync, self.sync_seq, self.ttl = node, False, 0, ttl
            self.expiry, self.expired_logged = now + ttl, False
            self._persist()                                      # durable before we answer
            return ({"ok": True, "epoch": self.epoch, "holder": node, "granted": True},
                    {"event": "lease_granted", "holder": node, "epoch": self.epoch,
                     "previous_holder": old, "ttl": ttl})

    def _refuse(self, reason, retry_in=None):
        reply = {"ok": False, "reason": reason, "epoch": self.epoch, "holder": self.holder}
        if retry_in is not None:
            reply["retry_in"] = round(retry_in, 2)
        return reply, None

    def status(self) -> dict:
        with self.lock:
            return {"epoch": self.epoch, "holder": self.holder, "in_sync": self.in_sync,
                    "lease_remaining": round(max(0.0, self.expiry - self.clock()), 2)
                    if self.holder else 0.0}

    def check_expiry(self):
        """Returns an event once when the holder's lease runs out (for the log)."""
        with self.lock:
            if self.holder and not self.expired_logged and self.clock() > self.expiry:
                self.expired_logged = True
                return {"event": "lease_expired", "holder": self.holder, "epoch": self.epoch,
                        "note": f"another node may take over after the {self.grace}s grace period"}
        return None


class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        state = self.server.state
        last_refusal = None
        try:
            for line in self.rfile:
                msg = None
                try:
                    msg = json.loads(line)
                    if msg.get("type") == "acquire":
                        reply, event = state.acquire(str(msg["node_id"]), int(msg["epoch"]),
                                                     float(msg["ttl"]), msg.get("in_sync"),
                                                     msg.get("sync_seq"))
                    elif msg.get("type") == "status":
                        reply, event = state.status(), None
                    else:
                        reply, event = {"ok": False, "reason": "unknown message type"}, None
                except (ValueError, KeyError, TypeError) as exc:
                    reply, event = {"ok": False, "reason": f"bad message: {exc!r}"}, None
                if event:
                    log_event(event.pop("event"), client_ip=self.client_address[0], **event)
                elif reply.get("ok") is False and reply.get("reason") != last_refusal:
                    last_refusal = reply.get("reason")
                    log_event("lease_refused", client_ip=self.client_address[0],
                              node_id=msg.get("node_id") if isinstance(msg, dict) else None,
                              reason=last_refusal)
                self.wfile.write((json.dumps(reply) + "\n").encode("utf-8"))
        except OSError:
            pass


class WitnessServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr, state: WitnessState):
        self.state = state
        super().__init__(addr, _Handler)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Witness / arbiter for the bank cluster (bonus 2)")
    ap.add_argument("--host", default=os.environ.get("BANK_WITNESS_HOST", "0.0.0.0"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("BANK_WITNESS_PORT", 7000)))
    ap.add_argument("--state", default=os.environ.get("BANK_WITNESS_STATE", "witness_state.json"))
    ap.add_argument("--log-file", default=os.environ.get("BANK_WITNESS_LOG", "logs/witness.log"))
    ap.add_argument("--grace", type=float, default=1.0,
                    help="extra seconds after a lease expires before another node may take over")
    ap.add_argument("--status", metavar="HOST:PORT",
                    help="do not run a witness: print the status of the witness at HOST:PORT")
    args = ap.parse_args(argv)
    if args.status:
        import socket
        host, port = args.status.rsplit(":", 1)
        with socket.create_connection((host, int(port)), timeout=3) as s:
            s.sendall(b'{"type": "status"}\n')
            print(s.makefile().readline().strip())
        return 0

    setup_logging(args.log_file)
    state = WitnessState(args.state, args.grace)
    server = WitnessServer((args.host, args.port), state)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log_event("witness_started", host=args.host, port=args.port, pid=os.getpid(),
              state_file=os.path.abspath(args.state), grace=args.grace, **state.status())
    try:
        while True:
            time.sleep(0.2)
            event = state.check_expiry()
            if event:
                log_event(event.pop("event"), **event)
    except KeyboardInterrupt:
        pass
    server.shutdown()
    log_event("witness_stopped")
    return 0


def _stop_on_sigterm(_signum, _frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _stop_on_sigterm)   # kill <pid> stops it cleanly too
    sys.exit(main())
