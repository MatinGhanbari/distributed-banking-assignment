"""Wire protocol: parsing and validating requests, building responses.

Every message is ONE JSON object on ONE line (UTF-8, terminated by "\\n").

Request : {"request_id": "optional", "action": "balance|deposit|withdraw|transfer",
           "account": "A1001", "amount": 100.50, "to_account": "A1002" (transfer only)}
Response: {"status": "success", "request_id": ..., "account": ..., "balance": ...}
          {"status": "error", "request_id": ..., "code": "...", "message": "..."}

Money is never handled as float: JSON numbers are parsed as Decimal and converted to
integer cents, which is also how balances are stored in the database.
"""
import json
from dataclasses import dataclass
from decimal import Decimal

ACTIONS = {"balance", "deposit", "withdraw", "transfer"}
MUTATING_ACTIONS = {"deposit", "withdraw", "transfer"}
MAX_AMOUNT_CENTS = 100_000_000_000      # 1,000,000,000.00 per single operation
MAX_AMOUNT = Decimal(MAX_AMOUNT_CENTS) / 100
MAX_FIELD_LEN = 128                     # max length of request_id / account strings
CENT = Decimal("0.01")


class BankError(Exception):
    """An expected, client-visible error. `code` is machine readable, `message` human readable."""

    def __init__(self, code, message, **extra):
        super().__init__(message)
        self.code = code
        self.message = message
        self.extra = extra              # e.g. current balance for INSUFFICIENT_FUNDS


@dataclass
class Request:
    request_id: str
    client_supplied_id: bool            # idempotency only applies to client-supplied ids
    action: str
    account: str
    amount_cents: int | None = None
    to_account: str | None = None
    simulate_failure: bool = False      # debug-only fault injection flag
    simulate_crash: bool = False        # debug-only: kill the process after replicating (bonus)


def parse_line(raw: bytes) -> dict:
    """Turn one raw line into a JSON object, or raise MALFORMED_REQUEST."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise BankError("MALFORMED_REQUEST", "request is not valid UTF-8 text")
    try:
        # parse_float=Decimal keeps 100.10 exact (no binary floating point rounding)
        obj = json.loads(text, parse_float=Decimal)
    except (ValueError, RecursionError):
        raise BankError("MALFORMED_REQUEST", "request is not valid JSON")
    if not isinstance(obj, dict):
        raise BankError("MALFORMED_REQUEST", "request must be a JSON object")
    return obj


def _valid_str(value) -> bool:
    return isinstance(value, str) and 0 < len(value) <= MAX_FIELD_LEN


def request_id_of(obj: dict, fallback: str) -> str:
    """Best-effort request id for echoing in responses (never raises)."""
    rid = obj.get("request_id")
    return rid if _valid_str(rid) else fallback


def parse_amount_cents(value) -> int:
    """Validate an amount: positive JSON number, max 2 decimals, not too large."""
    # bool is a subclass of int in Python, so reject it explicitly.
    # Strings, null, lists and NaN/Infinity (which json gives us as float) are rejected here too.
    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        raise BankError("INVALID_AMOUNT", "amount must be a positive number with at most 2 decimals")
    amount = Decimal(value)
    if amount <= 0:
        raise BankError("INVALID_AMOUNT", "amount must be greater than zero")
    if amount > MAX_AMOUNT:             # compare before multiplying: avoids overflow on 1e999999
        raise BankError("INVALID_AMOUNT", f"amount must not exceed {MAX_AMOUNT_CENTS // 100}")
    rounded = amount.quantize(CENT)
    if rounded != amount:               # exact comparison, e.g. 10.005 != 10.01
        raise BankError("INVALID_AMOUNT", "amount must have at most 2 decimal places")
    return int(rounded * 100)


def _require_str(obj: dict, field: str) -> str:
    value = obj.get(field)
    if value is None:
        raise BankError("MISSING_FIELD", f"missing required field '{field}'")
    if not _valid_str(value):
        raise BankError("MALFORMED_REQUEST",
                        f"field '{field}' must be a non-empty string (max {MAX_FIELD_LEN} chars)")
    return value


def validate(obj: dict, generated_id: str) -> Request:
    """Check the request object and return a normalised Request (or raise BankError)."""
    rid = obj.get("request_id")
    if rid is not None and not _valid_str(rid):
        raise BankError("MALFORMED_REQUEST", "request_id must be a non-empty string")

    action = _require_str(obj, "action")
    if action not in ACTIONS:
        raise BankError("UNSUPPORTED_ACTION",
                        f"unsupported action '{action}'; use one of {sorted(ACTIONS)}")
    req = Request(request_id=rid or generated_id, client_supplied_id=rid is not None,
                  action=action, account=_require_str(obj, "account"),
                  simulate_failure=obj.get("simulate_failure") is True,
                  simulate_crash=obj.get("simulate_crash") is True)

    if action in MUTATING_ACTIONS:
        if obj.get("amount") is None:
            raise BankError("MISSING_FIELD", "missing required field 'amount'")
        req.amount_cents = parse_amount_cents(obj["amount"])
    if action == "transfer":
        req.to_account = _require_str(obj, "to_account")
        if req.to_account == req.account:
            raise BankError("SAME_ACCOUNT", "cannot transfer to the same account")
    return req


def money(cents: int) -> float:
    """Cents -> JSON number for display (the exact value is also sent as *_cents)."""
    return round(cents / 100, 2)


def error_response(request_id: str, err: BankError, account=None) -> dict:
    resp = {"status": "error", "request_id": request_id, "code": err.code, "message": err.message}
    if account is not None:
        resp["account"] = account
    resp.update(err.extra)
    return resp
