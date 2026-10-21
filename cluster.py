"""Advanced Bonus: primary/backup replication with automatic failover.

Two servers (e.g. A = 192.168.75.10, B = 192.168.75.11). Exactly one is PRIMARY and
serves clients; the other is BACKUP, keeps an identical copy of the database and
rejects client requests with NOT_PRIMARY.

1. Synchronous replication: for every balance change the primary, still INSIDE its
   BEGIN IMMEDIATE transaction, assigns the next log sequence number (lsn), sends a
   replication record (lsn, epoch, request_id, new absolute balances, stored response)
   to the backup and waits until the backup has COMMITTED it. Only then does the
   primary commit and answer the client. So every acknowledged transaction is on both
   machines.
2. Failure detection: the primary sends a heartbeat every `heartbeat_interval` s. A
   backup that hears nothing for `failover_timeout` s promotes itself: epoch += 1,
   role = primary (both persisted), and starts serving clients.
3. Epochs: every message carries the sender's epoch. A node that learns of a higher
   epoch steps down to backup; records from a lower epoch are rejected (STALE_EPOCH).
4. Rejoin: a restarted node asks its peer first. If the peer is primary with an
   epoch >= its own, it becomes backup and receives a full snapshot before it is
   considered "in sync" (and only an in-sync backup may ever promote itself).

Peer messages use the same framing as clients: one JSON object per line, on the
separate replication port (default 6000). Every message gets exactly one reply.

Bonus 2 - split-brain prevention with a WITNESS (witness.py, --witness HOST:PORT):
5. A primary may serve (writes AND reads) only while it holds an unexpired LEASE from
   the witness. It renews every `lease_renew` s; its local deadline is computed from the
   time the renew request was SENT minus a safety margin, so it stops serving before the
   witness considers the lease expired. Without a lease it answers NO_QUORUM ("fenced").
6. A backup can become primary only through the witness (compare-and-set): the old
   lease must have expired (+ grace) and the old primary must have reported that this
   backup is in sync. The witness gives out the new epoch.
7. Before the primary acknowledges a write that is NOT replicated (degraded mode), the
   witness must know that the backup is out of sync - so the backup can never take over
   without those writes.
Without --witness, everything below works as in bonus 1.
"""
import json
import socket
import socketserver
import threading
import time
from contextlib import closing

import bank_db
from bank_db import get_state, set_state
from protocol import BankError


def parse_addr(text: str):
    host, port = text.rsplit(":", 1)
    return host, int(port)


class PeerConnection:
    """A request/response connection to the peer's replication port."""

    def __init__(self, addr, timeout: float):
        self.sock = socket.create_connection(addr, timeout=timeout)
        self.rfile = self.sock.makefile("rb")

    def call(self, msg: dict) -> dict:
        self.sock.sendall((json.dumps(msg) + "\n").encode("utf-8"))
        line = self.rfile.readline()
        if not line:
            raise ConnectionError("peer closed the connection")
        return json.loads(line)

    def close(self):
        for f in (self.rfile.close, self.sock.close):
            try:
                f()
            except OSError:
                pass


class Cluster:
    def __init__(self, cfg: dict, log_event):
        self.cfg, self.log = cfg, log_event
        self.node_id = cfg["node_id"]
        self.peer = parse_addr(cfg["peer"])
        self.peer_client = cfg["peer_client"] or f"{self.peer[0]}:{cfg['port']}"
        self.db_path = cfg["db_path"]
        self.witness = parse_addr(cfg["witness"]) if cfg.get("witness") else None
        self.state_lock = threading.Lock()   # protects role / epoch / synced / last_contact
        self.link_lock = threading.Lock()    # protects self.link (primary -> backup socket)
        self.link = None
        self.degraded = False
        self.stopping = threading.Event()

        with closing(bank_db.connect(self.db_path)) as conn:
            conn.execute("BEGIN IMMEDIATE")
            if get_state(conn, "role") is None:  # first start of this node: use --initial-role
                set_state(conn, "role", cfg["initial_role"])
                # With a witness, every epoch (also the first) is handed out by the witness.
                first_epoch = 1 if cfg["initial_role"] == "primary" and not self.witness else 0
                set_state(conn, "epoch", first_epoch)
                set_state(conn, "last_lsn", 0)
            self.role = get_state(conn, "role")
            self.epoch = int(get_state(conn, "epoch"))
            self.last_lsn = int(get_state(conn, "last_lsn"))
            conn.execute("COMMIT")
        # A primary's own data is by definition the reference copy. A backup is only
        # "in sync" after it received a snapshot from the current primary.
        self.synced = self.role == "primary"
        self.last_contact = time.monotonic()
        # --- bonus 2 (witness mode only) ---
        self.lease_deadline = 0.0     # monotonic time until which we may serve as primary
        self.lease_lost_logged = False
        self.sync_up = False          # is the replication link to the backup up (= in sync)?
        self.sync_seq = 0             # +1 on every link up/down change (orders the reports)
        self.witness_seq = 0          # highest sync_seq the witness has acknowledged
        self.last_promote_try = 0.0
        self.last_refusal = None

    # ------------------------------------------------------------------ lifecycle
    def start(self):
        """Start the replication listener, decide our role, start background threads."""
        self.listener = _PeerServer((self.cfg["repl_host"], self.cfg["repl_port"]), self)
        threading.Thread(target=self.listener.serve_forever, daemon=True).start()
        self.log("cluster_start", node_id=self.node_id, role=self.role, epoch=self.epoch,
                 last_lsn=self.last_lsn, repl_port=self.cfg["repl_port"], peer=self.cfg["peer"])
        if self.witness:
            self._startup_witness()
            threading.Thread(target=self._lease_loop, daemon=True).start()
        else:
            self._startup_probe()
        threading.Thread(target=self._primary_loop, daemon=True).start()
        threading.Thread(target=self._detector_loop, daemon=True).start()

    def stop(self):
        self.stopping.set()
        self.listener.shutdown()
        self.listener.server_close()
        self._close_link()

    def is_primary(self) -> bool:
        return self.role == "primary"

    def lease_ok(self) -> bool:
        """Without a witness there is no lease (bonus 1 behaviour): always True."""
        return self.witness is None or time.monotonic() < self.lease_deadline

    def check_can_serve(self):
        """Raise the right error if this node must not answer a client request now."""
        if self.role != "primary":
            raise self.not_primary_error()
        if not self.lease_ok():
            raise self.no_quorum_error()

    def no_quorum_error(self, why="it has no valid lease from the witness") -> BankError:
        return BankError("NO_QUORUM", f"node {self.node_id} may not serve: {why}",
                         node_id=self.node_id, primary_hint=self.peer_client)

    def not_primary_error(self) -> BankError:
        return BankError("NOT_PRIMARY", f"node {self.node_id} is a backup, not the primary",
                         node_id=self.node_id, role=self.role, primary_hint=self.peer_client)

    def my_info(self) -> dict:
        return {"node_id": self.node_id, "role": self.role, "epoch": self.epoch,
                "last_lsn": self.last_lsn, "synced": self.synced,
                "initial_role": self.cfg["initial_role"], "lease": self.lease_ok()}

    # --------------------------------------------------------------- role changes
    def _persist_role(self, role: str, epoch: int):
        """Write role/epoch to the database. Must not be called inside a DB transaction."""
        with closing(bank_db.connect(self.db_path)) as conn:
            conn.execute("BEGIN IMMEDIATE")
            set_state(conn, "role", role)
            set_state(conn, "epoch", epoch)
            conn.execute("COMMIT")

    def step_down(self, epoch: int, reason: str):
        """Primary -> backup because a newer primary exists. Our data must be resynced."""
        with self.state_lock:                     # check-and-set in one step
            if self.role != "primary":
                return
            self.role, self.synced = "backup", False
            self.epoch = max(epoch, self.epoch)
            self.last_contact = time.monotonic()
            self.lease_deadline = 0.0
            new_epoch = self.epoch
        self._close_link()
        self._persist_role("backup", new_epoch)
        self.log("role_change", node_id=self.node_id, old_role="primary", new_role="backup",
                 epoch=new_epoch, reason=reason)

    def _promote(self, reason: str):
        with closing(bank_db.connect(self.db_path)) as conn:
            conn.execute("BEGIN IMMEDIATE")      # serialised with replication records
            epoch = int(get_state(conn, "epoch")) + 1
            set_state(conn, "role", "primary")
            set_state(conn, "epoch", epoch)
            conn.execute("COMMIT")
        with self.state_lock:
            self.role, self.epoch, self.synced = "primary", epoch, True
        self.degraded = False
        self.log("promoted", node_id=self.node_id, epoch=epoch, last_lsn=self.last_lsn, reason=reason)

    def _startup_probe(self):
        """Ask the peer for its role before serving anything (prevents two primaries)."""
        info, deadline = None, time.monotonic() + self.cfg["startup_probe"]
        while info is None and not self.stopping.is_set():
            try:
                conn = PeerConnection(self.peer, timeout=1.0)
                try:
                    info = conn.call({"type": "hello", **self.my_info()})
                finally:
                    conn.close()
            except (OSError, ValueError):
                if time.monotonic() > deadline:
                    break
                time.sleep(0.3)
        if info is None:
            self.log("startup_probe", peer_reachable=False, role=self.role, epoch=self.epoch,
                     note="keeping persisted role" + ("" if self.role == "primary" else
                                                       "; waiting for a primary to sync from"))
            return
        self.log("startup_probe", peer_reachable=True, peer=info, my_role=self.role, my_epoch=self.epoch)
        if info["role"] == "primary":
            if info["epoch"] >= self.epoch or self.role == "backup":
                # A backup starts unsynced anyway (and may already have received a snapshot
                # while we were probing - do not undo that). A primary steps down here.
                self.step_down(info["epoch"], f"peer {info['node_id']} is primary with epoch {info['epoch']}")
        elif self.role == "backup":
            # Both are backups (e.g. both restarted): the one with the newest data takes over.
            mine = (self.epoch, self.last_lsn, self.cfg["initial_role"] == "primary")
            theirs = (info["epoch"], info["last_lsn"], info.get("initial_role") == "primary")
            if mine > theirs:
                self._promote("both nodes were backup; this node has the newest data")

    # ---------------------------------------------------- primary side: replicate
    def replicate(self, conn, request_id, balances: dict, processed_row):
        """Called by Bank.execute INSIDE the client's transaction, before COMMIT."""
        self.check_can_serve()                    # role + (bonus 2) lease
        lsn = int(get_state(conn, "last_lsn")) + 1
        set_state(conn, "last_lsn", lsn)          # committed together with the balances
        record = {"type": "record", "node_id": self.node_id, "epoch": self.epoch, "lsn": lsn,
                  "request_id": request_id, "balances": balances, "processed": processed_row}
        resp = None
        with self.link_lock:
            # Updated under link_lock, so a heartbeat never announces an lsn whose record
            # has not been sent yet.
            self.last_lsn = lsn
            if self.link is not None:
                try:
                    resp = self.link.call(record)
                except (OSError, ValueError) as exc:
                    self._drop_link_locked(f"replication of lsn {lsn} failed: {exc!r}")
        if resp is not None and resp.get("ok"):
            return                                # on the backup: safe to commit
        if resp is not None and resp.get("error") == "STALE_EPOCH":
            err = self.not_primary_error()        # the peer is a newer primary: we must stop
            err.step_down_epoch = resp["epoch"]   # server.py steps down after the ROLLBACK
            raise err
        if resp is not None:
            self._drop_link(f"backup refused lsn {lsn}: {resp.get('error')}")
        # Degraded: this write exists only here. Bonus 2: the witness must know that the
        # backup is out of sync BEFORE we commit, or the backup could take over without it.
        self._witness_must_know_out_of_sync()

    def _drop_link_locked(self, reason: str):
        if self.link is not None:
            self.link.close()
            self.link = None
            self._set_sync(False)
        if self.role == "primary" and not self.degraded:
            self.degraded = True
            self.log("degraded_mode", node_id=self.node_id, epoch=self.epoch, reason=reason,
                     note="serving WITHOUT a backup copy until the backup is back")

    def _drop_link(self, reason: str):
        with self.link_lock:
            self._drop_link_locked(reason)

    def _close_link(self):
        with self.link_lock:
            if self.link is not None:
                self.link.close()
                self.link = None

    def _primary_loop(self):
        """While primary: keep a link to the backup (snapshot on connect) and send heartbeats."""
        while not self.stopping.wait(self.cfg["heartbeat_interval"]):
            if self.role != "primary":
                continue
            if self.link is None:
                self._connect_backup()
            else:
                self._heartbeat()

    def _connect_backup(self):
        peer = None
        try:
            peer = PeerConnection(self.peer, timeout=self.cfg["repl_timeout"])
            # Cheap liveness/role check first, WITHOUT holding the database lock, so a
            # hanging peer cannot block our clients.
            info = peer.call({"type": "hello", **self.my_info()})
        except (OSError, ValueError) as exc:
            if peer is not None:
                peer.close()
            self._drop_link(f"backup unreachable: {exc!r}")
            return
        if info.get("role") == "primary" and info.get("epoch", 0) >= self.epoch:
            peer.close()
            self.step_down(info["epoch"], f"peer {info.get('node_id')} is primary with epoch {info['epoch']}")
            return
        result, snapshot = None, None
        with closing(bank_db.connect(self.db_path)) as conn:
            # Hold the write lock while the snapshot is sent: no client transaction can
            # commit in between, so after this the backup is exactly in sync.
            conn.execute("BEGIN IMMEDIATE")
            try:
                snapshot = {"type": "snapshot", "node_id": self.node_id, "epoch": self.epoch,
                            "lsn": int(get_state(conn, "last_lsn")),
                            "accounts": conn.execute("SELECT * FROM accounts").fetchall(),
                            "processed": conn.execute("SELECT * FROM processed_requests").fetchall()}
                peer.sock.settimeout(max(5.0, self.cfg["repl_timeout"]))
                result = peer.call(snapshot)
                peer.sock.settimeout(self.cfg["repl_timeout"])
                if result.get("ok") and self.role == "primary":
                    with self.link_lock:
                        self.link = peer
                    self._set_sync(True)          # witness learns it with the next renew
            except (OSError, ValueError) as exc:
                result = {"ok": False, "error": repr(exc)}
            finally:
                conn.execute("ROLLBACK")          # read-only transaction
        if result.get("ok") and self.link is peer:
            self.degraded = False
            self.log("snapshot_sent", node_id=self.node_id, epoch=self.epoch, lsn=snapshot["lsn"],
                     accounts=len(snapshot["accounts"]), processed=len(snapshot["processed"]),
                     note="backup in sync, synchronous replication active")
            if self.witness:
                self._renew_lease()               # tell the witness right away: in sync again
            return
        peer.close()
        if result.get("error") == "STALE_EPOCH":
            self.step_down(result["epoch"], "peer is a primary with a higher epoch")
        else:
            self._drop_link(f"snapshot failed: {result.get('error')}")

    def _heartbeat(self):
        with self.link_lock:
            if self.link is None:
                return
            try:
                resp = self.link.call({"type": "heartbeat", "node_id": self.node_id,
                                       "epoch": self.epoch, "lsn": self.last_lsn,
                                       "lease": self.lease_ok()})
            except (OSError, ValueError) as exc:
                self._drop_link_locked(f"heartbeat failed: {exc!r}")
                return
        if resp.get("error") == "STALE_EPOCH":
            self.step_down(resp["epoch"], "peer is a primary with a higher epoch")
        elif not resp.get("ok"):
            self._drop_link(f"backup asked for a new snapshot: {resp.get('error')}")

    # ------------------------------------------------------ backup side: receive
    def _detector_loop(self):
        """Backup: promote ourselves if the primary is silent for failover_timeout seconds."""
        warned = False
        while not self.stopping.wait(0.1):
            with self.state_lock:
                role, synced = self.role, self.synced
                silent = time.monotonic() - self.last_contact
            if role != "backup" or silent < self.cfg["failover_timeout"]:
                warned = False
                continue
            if not synced:
                if not warned:
                    self.log("cannot_promote", node_id=self.node_id, silent_s=round(silent, 2),
                             reason="this backup never received a snapshot, its data may be old")
                    warned = True
                continue
            if self.witness:
                if self.epoch == 0:
                    if not warned:
                        self.log("cannot_promote", node_id=self.node_id, silent_s=round(silent, 2),
                                 reason="no primary has held a lease yet (waiting for the "
                                        "initial primary)")
                        warned = True
                elif time.monotonic() - self.last_promote_try >= 0.5:
                    self._promote_via_witness(silent)
                continue
            self.log("primary_failure_detected", node_id=self.node_id, silent_s=round(silent, 2))
            self._promote(f"no heartbeat from primary for {silent:.1f}s")

    def handle_peer_message(self, conn, msg: dict) -> dict:
        kind = msg.get("type")
        if kind == "hello":
            if msg.get("role") == "primary" and msg["epoch"] > self.epoch and self.role == "primary":
                self.step_down(msg["epoch"], f"peer {msg['node_id']} is primary with a higher epoch")
            return self.my_info()
        if kind == "heartbeat":
            return self._on_heartbeat(msg)
        if kind == "status":                     # read-only, used by the demos
            return {**self.my_info(), **bank_db.digest(conn)}
        if kind in ("record", "snapshot"):
            return self._apply(conn, msg)
        return {"ok": False, "error": "UNKNOWN_MESSAGE"}

    def _check_epoch(self, epoch: int):
        """Return an error reply if a message from this epoch must be refused."""
        if epoch < self.epoch or (epoch == self.epoch and self.role == "primary"):
            return {"ok": False, "error": "STALE_EPOCH", "epoch": self.epoch}
        return None

    def _on_heartbeat(self, msg):
        refused = self._check_epoch(msg["epoch"])
        if refused:
            return refused
        if self.role == "primary":               # a newer primary exists
            self.step_down(msg["epoch"], f"heartbeat from newer primary {msg['node_id']}")
        adopt = None
        with self.state_lock:
            if msg.get("lease", True):           # a fenced primary (no lease) counts as silent
                self.last_contact = time.monotonic()
            if msg["lsn"] > self.last_lsn:       # primary wrote something we never got
                self.synced = False
            if self.synced and msg["epoch"] > self.epoch and msg["lsn"] == self.last_lsn:
                # Same data, newer epoch (e.g. the primary got its first lease after it sent
                # us the snapshot): remember the epoch so we may take over from it.
                self.epoch = adopt = msg["epoch"]
        if adopt is not None:
            with closing(bank_db.connect(self.db_path)) as c:
                c.execute("BEGIN IMMEDIATE")      # only if nothing newer happened meanwhile
                if get_state(c, "role") == "backup" and int(get_state(c, "epoch")) < adopt:
                    set_state(c, "epoch", adopt)
                c.execute("COMMIT")
        if not self.synced:
            return {"ok": False, "error": "NEED_SNAPSHOT"}
        return {"ok": True}

    def _apply(self, conn, msg):
        """Apply a snapshot or one record in ONE local transaction; ack only after COMMIT."""
        refused = self._check_epoch(msg["epoch"])
        if refused:
            return refused
        if self.role == "primary":               # sender is a newer primary: we step down
            self.step_down(msg["epoch"], f"replication from newer primary {msg['node_id']}")
        conn.execute("BEGIN IMMEDIATE")
        try:
            # Re-check with the persisted values (we might have been promoted meanwhile).
            epoch = int(get_state(conn, "epoch"))
            last = int(get_state(conn, "last_lsn"))
            if msg["epoch"] < epoch or (msg["epoch"] == epoch and get_state(conn, "role") == "primary"):
                conn.execute("ROLLBACK")
                return {"ok": False, "error": "STALE_EPOCH", "epoch": epoch}
            if msg["type"] == "snapshot":
                conn.execute("DELETE FROM accounts")
                conn.executemany("INSERT INTO accounts VALUES (?, ?)", msg["accounts"])
                conn.execute("DELETE FROM processed_requests")
                conn.executemany("INSERT INTO processed_requests VALUES (?, ?, ?, ?)", msg["processed"])
                new_lsn = msg["lsn"]
            else:
                if msg["lsn"] <= last:           # duplicate (resent): ack, do not apply again
                    conn.execute("ROLLBACK")
                    self._touch()
                    return {"ok": True, "lsn": msg["lsn"], "duplicate": True}
                if not self.synced or msg["lsn"] != last + 1:
                    conn.execute("ROLLBACK")     # we missed something: need a full snapshot
                    with self.state_lock:
                        self.synced = False
                    return {"ok": False, "error": "NEED_SNAPSHOT", "last_lsn": last}
                for account, cents in msg["balances"].items():
                    if conn.execute("UPDATE accounts SET balance_cents = ? WHERE account_id = ?",
                                    (cents, account)).rowcount == 0:
                        conn.execute("INSERT INTO accounts VALUES (?, ?)", (account, cents))
                if msg["processed"]:
                    conn.execute("INSERT OR REPLACE INTO processed_requests VALUES (?, ?, ?, ?)",
                                 msg["processed"])
                new_lsn = msg["lsn"]
            set_state(conn, "last_lsn", new_lsn)
            set_state(conn, "epoch", msg["epoch"])
            set_state(conn, "role", "backup")
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        with self.state_lock:
            self.role, self.epoch, self.last_lsn = "backup", msg["epoch"], new_lsn
            if msg["type"] == "snapshot":
                self.synced = True
        self._touch()
        if msg["type"] == "snapshot":
            self.log("snapshot_applied", node_id=self.node_id, from_node=msg["node_id"],
                     epoch=msg["epoch"], lsn=new_lsn, accounts=len(msg["accounts"]),
                     processed=len(msg["processed"]))
        else:
            self.log("replica_applied", node_id=self.node_id, lsn=new_lsn,
                     request_id=msg["request_id"], balances=msg["balances"])
        return {"ok": True, "lsn": new_lsn}

    def _touch(self):
        with self.state_lock:
            self.last_contact = time.monotonic()


    # ------------------------------------------------- bonus 2: witness and lease
    def _witness_call(self, msg: dict) -> dict:
        conn = PeerConnection(self.witness, timeout=self.cfg["witness_timeout"])
        try:
            return conn.call(msg)
        finally:
            conn.close()

    def _set_sync(self, up: bool):
        """Record a change of the link state; the witness hears about it with sync_seq."""
        with self.state_lock:
            if self.sync_up != up:
                self.sync_up = up
                self.sync_seq += 1

    def _startup_witness(self):
        """A (re)starting node asks the witness who the primary is before serving."""
        status, deadline = None, time.monotonic() + self.cfg["startup_probe"]
        while status is None and not self.stopping.is_set():
            try:
                status = self._witness_call({"type": "status"})
            except (OSError, ValueError):
                if time.monotonic() > deadline:
                    break
                time.sleep(0.3)
        if status is None:
            self.log("startup_witness", reachable=False, role=self.role, epoch=self.epoch,
                     note="no lease without the witness: this node will not serve as primary "
                          "until the witness answers")
            return
        self.log("startup_witness", reachable=True, witness=status, my_role=self.role,
                 my_epoch=self.epoch)
        holder = status.get("holder")
        if holder not in (None, self.node_id):
            # Someone else is (or was last) primary: we are backup and must resync from it.
            self.step_down(status["epoch"], f"witness says {holder} is primary at epoch "
                                            f"{status['epoch']}")
        elif holder == self.node_id and self.role != "primary":
            # The witness granted us the lease but we crashed before persisting it.
            self._persist_role("primary", status["epoch"])
            with self.state_lock:
                self.role, self.epoch, self.synced = "primary", status["epoch"], True

    def _lease_loop(self):
        """Primary: renew the lease every lease_renew seconds; log when we become fenced."""
        reason = None
        while not self.stopping.is_set():
            wait = self.cfg["lease_renew"]
            if self.role == "primary":
                if reason is not None and not self.lease_ok() and not self.lease_lost_logged:
                    self.lease_lost_logged = True
                    self.log("lease_lost", node_id=self.node_id, epoch=self.epoch, reason=reason,
                             note="FENCED: refusing all client requests with NO_QUORUM")
                reason = self._renew_lease()
                remaining = self.lease_deadline - time.monotonic()
                if reason is not None and remaining > 0:
                    # Still valid: wake up exactly when it runs out, so the log shows the
                    # real moment we stopped serving.
                    wait = min(wait, remaining + 0.005)
                elif reason is not None:
                    wait = min(wait, 0.2) if not self.lease_lost_logged else wait
            else:
                reason = None
            self.stopping.wait(wait)

    def _renew_lease(self):
        """Ask the witness for (a renewal of) our lease. Returns None on success, else why not."""
        with self.state_lock:
            epoch, seq, up = self.epoch, self.sync_seq, self.sync_up
        sent_at = time.monotonic()               # the lease counts from the SEND time
        try:
            resp = self._witness_call({"type": "acquire", "node_id": self.node_id, "epoch": epoch,
                                       "ttl": self.cfg["lease_ttl"], "in_sync": up,
                                       "sync_seq": seq})
        except (OSError, ValueError) as exc:
            return f"witness unreachable: {exc!r}"
        if not resp.get("ok"):
            if resp.get("holder") not in (None, self.node_id):
                self.step_down(resp["epoch"], f"witness: {resp['holder']} holds the lease at "
                                              f"epoch {resp['epoch']}")
            return resp.get("reason", "refused")
        if resp["epoch"] != epoch:               # granted (bootstrap) or adopted a newer epoch
            self._persist_role("primary", resp["epoch"])
        with self.state_lock:
            if self.role != "primary":
                return "stepped down meanwhile"
            self.epoch = resp["epoch"]
            if resp.get("granted"):
                self.witness_seq = 0             # the witness reset its sync info
                self.log("lease_granted", node_id=self.node_id, epoch=self.epoch)
            else:
                self.witness_seq = max(self.witness_seq, seq)
            # Conservative: the witness started counting AFTER we sent, we count from send
            # time and stop `lease_margin` seconds early on top of that.
            self.lease_deadline = sent_at + self.cfg["lease_ttl"] - self.cfg["lease_margin"]
        if self.lease_lost_logged:
            self.lease_lost_logged = False
            self.log("lease_restored", node_id=self.node_id, epoch=self.epoch)
        return None

    def _witness_must_know_out_of_sync(self):
        """Degraded write: make sure the witness knows the backup is behind, else refuse."""
        if self.witness is None:
            return                               # bonus 1: just continue alone
        with self.state_lock:
            known = self.witness_seq >= self.sync_seq
        if known:
            return
        reason = self._renew_lease()             # carries in_sync=false + the new sync_seq
        with self.state_lock:
            known = self.witness_seq >= self.sync_seq
        if reason is not None or not known:
            raise self.no_quorum_error("the backup is unreachable and the witness could not be "
                                       "told so; refusing to write without either")

    def _promote_via_witness(self, silent: float):
        """Backup: ask the witness for the lease (compare-and-set on the epoch)."""
        self.last_promote_try = time.monotonic()
        sent_at = time.monotonic()
        try:
            resp = self._witness_call({"type": "acquire", "node_id": self.node_id,
                                       "epoch": self.epoch, "ttl": self.cfg["lease_ttl"]})
        except (OSError, ValueError) as exc:
            resp = {"ok": False, "reason": f"witness unreachable: {exc!r}"}
        if not resp.get("ok"):
            if resp.get("reason", "").split(" (")[0] != self.last_refusal:
                self.last_refusal = resp.get("reason", "").split(" (")[0]
                self.log("promotion_refused", node_id=self.node_id, silent_s=round(silent, 2),
                         reason=resp.get("reason"))
            return
        self.last_refusal = None
        with closing(bank_db.connect(self.db_path)) as conn:
            conn.execute("BEGIN IMMEDIATE")      # serialised with replication records
            set_state(conn, "role", "primary")
            set_state(conn, "epoch", resp["epoch"])
            conn.execute("COMMIT")
        with self.state_lock:
            self.role, self.epoch, self.synced = "primary", resp["epoch"], True
            self.sync_up, self.sync_seq, self.witness_seq = False, 0, 0
            self.lease_deadline = sent_at + self.cfg["lease_ttl"] - self.cfg["lease_margin"]
            self.lease_lost_logged = False
        self.degraded = False
        self.log("promoted", node_id=self.node_id, epoch=resp["epoch"], last_lsn=self.last_lsn,
                 reason=f"no heartbeat from a lease-holding primary for {silent:.1f}s; "
                        "the witness granted the lease (2 of 3 votes)")


class _PeerHandler(socketserver.StreamRequestHandler):
    """Serves one connection from the peer on the replication port."""

    def handle(self):
        cluster = self.server.cluster
        conn = bank_db.connect(cluster.db_path)
        try:
            for line in self.rfile:
                try:
                    reply = cluster.handle_peer_message(conn, json.loads(line))
                except (ValueError, KeyError, TypeError) as exc:
                    reply = {"ok": False, "error": f"BAD_MESSAGE {exc!r}"}
                self.wfile.write((json.dumps(reply) + "\n").encode("utf-8"))
        except OSError:
            pass                                  # peer went away
        finally:
            conn.close()


class _PeerServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr, cluster: Cluster):
        self.cluster = cluster
        super().__init__(addr, _PeerHandler)
