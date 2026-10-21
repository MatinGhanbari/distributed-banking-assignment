"""Shared state: the SQLite database holding all accounts.

ACID strategy (see docs/design-notes.md):
  * every balance change runs inside BEGIN IMMEDIATE ... COMMIT (atomic + isolated:
    IMMEDIATE takes the database write lock up-front, so writers are serialised),
  * ROLLBACK on any exception (nothing half-done is ever stored),
  * CHECK(balance_cents >= 0) in the schema (consistency enforced by the DB itself),
  * WAL journal + synchronous=FULL, and we answer the client only after COMMIT
    returned (durability).
Each client thread owns its own Bank object / sqlite3 connection (connections are
not shared between threads).
"""
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

from protocol import BankError, Request, money

# Simulated, made-up accounts created on first run (balances in cents).
SEED_ACCOUNTS = {"A1001": 100_000, "A1002": 50_000, "A1003": 250_000}

SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    account_id    TEXT PRIMARY KEY,
    balance_cents INTEGER NOT NULL CHECK (balance_cents >= 0)
);
-- Idempotency: one row per successfully applied client request_id.
CREATE TABLE IF NOT EXISTS processed_requests (
    request_id    TEXT PRIMARY KEY,
    fingerprint   TEXT NOT NULL,      -- action/account/amount, detects id reuse
    response_json TEXT NOT NULL,      -- the exact response we sent the first time
    created_at    TEXT NOT NULL
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    # isolation_level=None: we issue BEGIN/COMMIT/ROLLBACK ourselves.
    # timeout=30: wait up to 30 s for the write lock instead of failing with "database is locked".
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    return conn


def init_db(db_path: str) -> None:
    """Create tables and seed the demo accounts if the database is empty."""
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.execute("BEGIN IMMEDIATE")
        if conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0] == 0:
            conn.executemany("INSERT INTO accounts VALUES (?, ?)", SEED_ACCOUNTS.items())
        conn.execute("COMMIT")
    finally:
        conn.close()


class Bank:
    """Executes validated requests against the database (one instance per client thread)."""

    def __init__(self, db_path: str, fault_injection: bool = False):
        self.conn = connect(db_path)
        self.fault_injection = fault_injection

    def close(self):
        self.conn.close()

    @contextmanager
    def _transaction(self):
        """BEGIN IMMEDIATE ... COMMIT, or ROLLBACK if anything inside raised."""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.conn.execute("COMMIT")          # durable once this returns
        except BaseException:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
            raise

    # ---- small helpers, always called inside a transaction ---------------------------
    def _balance(self, account: str) -> int:
        row = self.conn.execute("SELECT balance_cents FROM accounts WHERE account_id = ?",
                                (account,)).fetchone()
        if row is None:
            raise BankError("UNKNOWN_ACCOUNT", f"account '{account}' does not exist")
        return row[0]

    def _debit(self, account: str, cents: int) -> None:
        balance = self._balance(account)
        # Conditional update: the balance check and the change are one statement.
        cur = self.conn.execute(
            "UPDATE accounts SET balance_cents = balance_cents - ? "
            "WHERE account_id = ? AND balance_cents >= ?", (cents, account, cents))
        if cur.rowcount == 0:
            raise BankError("INSUFFICIENT_FUNDS", f"insufficient funds in '{account}'",
                            balance=money(balance), balance_cents=balance)

    def _credit(self, account: str, cents: int) -> None:
        self.conn.execute("UPDATE accounts SET balance_cents = balance_cents + ? "
                          "WHERE account_id = ?", (cents, account))

    def _maybe_fail(self, req: Request) -> None:
        """Debug-only fault injection: crash in the middle of the transaction."""
        if self.fault_injection and req.simulate_failure:
            raise RuntimeError("simulated failure after the first balance update")

    # ---- public entry point -------------------------------------------------------
    def execute(self, req: Request) -> dict:
        if req.simulate_failure and not self.fault_injection:
            # Refuse instead of silently ignoring the flag, so a demo never moves real money.
            raise BankError("FAULT_INJECTION_DISABLED",
                            "simulate_failure requires a server started with fault injection enabled")
        if req.action == "balance":
            # A single SELECT is atomic on its own; WAL gives it a consistent snapshot.
            return self._ok(req, self._balance(req.account))

        fingerprint = json.dumps([req.action, req.account, req.to_account, req.amount_cents])
        with self._transaction():
            if req.client_supplied_id:
                row = self.conn.execute(
                    "SELECT fingerprint, response_json FROM processed_requests "
                    "WHERE request_id = ?", (req.request_id,)).fetchone()
                if row is not None:
                    if row[0] != fingerprint:
                        raise BankError("DUPLICATE_REQUEST_ID",
                                        "request_id was already used for a different operation")
                    stored = json.loads(row[1])
                    stored["replayed"] = True    # already applied once: do NOT apply again
                    return stored

            if req.action == "deposit":
                self._balance(req.account)       # raises UNKNOWN_ACCOUNT
                self._credit(req.account, req.amount_cents)
                self._maybe_fail(req)
            elif req.action == "withdraw":
                self._debit(req.account, req.amount_cents)
                self._maybe_fail(req)
            else:  # transfer: debit + credit in the SAME transaction
                self._balance(req.to_account)    # check destination before touching money
                self._debit(req.account, req.amount_cents)
                self._maybe_fail(req)            # crash between debit and credit -> rollback
                self._credit(req.to_account, req.amount_cents)

            resp = self._ok(req, self._balance(req.account))
            if req.action == "transfer":
                to_cents = self._balance(req.to_account)
                resp.update(to_account=req.to_account, to_balance=money(to_cents),
                            to_balance_cents=to_cents)
            if req.client_supplied_id:
                # Stored in the same transaction as the balance change: both or neither.
                self.conn.execute(
                    "INSERT INTO processed_requests VALUES (?, ?, ?, ?)",
                    (req.request_id, fingerprint, json.dumps(resp),
                     datetime.now(timezone.utc).isoformat()))
        return resp                               # only reached after COMMIT succeeded

    @staticmethod
    def _ok(req: Request, cents: int) -> dict:
        resp = {"status": "success", "request_id": req.request_id, "action": req.action,
                "account": req.account, "balance": money(cents), "balance_cents": cents}
        if req.amount_cents is not None:
            resp["amount"] = money(req.amount_cents)
        return resp
