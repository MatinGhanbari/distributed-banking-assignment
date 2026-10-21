# Mini Banking Transaction Server (Distributed Systems – Assignment 1)

A multi-client banking server over raw TCP sockets. Clients check balances, deposit,
withdraw and transfer money on shared (simulated) accounts. The server stays correct
when many clients act at once, rejects bad input with clear errors, and keeps committed
data after a crash.

* **Language:** Python 3.12, standard library only (no `pip install`), Ubuntu 24.04
* **Protocol:** TCP, newline-delimited JSON (one UTF-8 JSON object per line)
* **Concurrency:** one thread per client (`socketserver.ThreadingTCPServer`), limited by `max_clients`
* **Shared state:** SQLite (WAL, `synchronous=FULL`), every change in a `BEGIN IMMEDIATE` transaction

The design is explained in [docs/design-notes.md](docs/design-notes.md).

## Files

```
server.py              TCP server: config, threads, MAX_CLIENTS, logging, shutdown
protocol.py            request parsing/validation (amounts as Decimal -> integer cents), responses
bank_db.py             SQLite schema, seeding, ACID transactions, idempotency
client.py              interactive / one-shot client (also used by demos and tests)
config.json            default configuration
demo/                  demo scripts (see below)
tests/test_bank.py     automated tests (unittest)
docs/design-notes.md   design notes for the report
```

## Protocol

Request (one line):
```json
{"request_id": "optional-unique-id", "action": "withdraw", "account": "A1001", "amount": 30}
```
`action` is `balance`, `deposit`, `withdraw` or `transfer` (transfer also needs `to_account`).
`amount` must be a JSON number > 0 with at most 2 decimals. If `request_id` is missing the
server generates one (`srv-...`).

Responses:
```json
{"status": "success", "request_id": "r1", "action": "withdraw", "account": "A1001", "balance": 970.0, "balance_cents": 97000, "amount": 30.0}
{"status": "error", "request_id": "r2", "code": "INSUFFICIENT_FUNDS", "message": "insufficient funds in 'A1002'", "account": "A1002", "balance": 500.0, "balance_cents": 50000}
```

| Error code | When |
|---|---|
| `UNKNOWN_ACCOUNT` | account / to_account does not exist (e.g. `Z9999`) |
| `INSUFFICIENT_FUNDS` | withdraw/transfer larger than the balance |
| `INVALID_AMOUNT` | `-50`, `0`, `"abc"`, `true`, `NaN`, `Infinity`, `10.005`, > 1,000,000,000 |
| `UNSUPPORTED_ACTION` | action is not one of the four above |
| `MISSING_FIELD` | `action`, `account`, `amount` or `to_account` missing |
| `MALFORMED_REQUEST` | broken JSON, JSON that is not an object, non-UTF-8 bytes, line > 64 KB, wrong field types |
| `SAME_ACCOUNT` | transfer to the same account |
| `DUPLICATE_REQUEST_ID` | a `request_id` reused for a *different* operation |
| `SERVER_BUSY` | sent on connect when `max_clients` connections are already active, then closed |
| `FAULT_INJECTION_DISABLED` | `simulate_failure` sent to a server without fault injection |
| `INTERNAL_ERROR` | unexpected exception; the transaction was rolled back |

After an error (even random bytes) the connection stays open for the next line.

Seeded demo accounts (made-up data, created when the database is empty):
`A1001 = 1000.00`, `A1002 = 500.00`, `A1003 = 2500.00`.

## Configuration

Precedence: **command line > environment variable > config.json > built-in default**.

| config.json | env variable | CLI | default |
|---|---|---|---|
| `host` | `BANK_HOST` | `--host` | `0.0.0.0` |
| `port` | `BANK_PORT` | `--port` | `5000` |
| `max_clients` | `BANK_MAX_CLIENTS` | `--max-clients` | `100` |
| `db_path` | `BANK_DB_PATH` | `--db` | `bank.db` |
| `log_file` | `BANK_LOG_FILE` | `--log-file` | `logs/server.log` |
| `max_line_bytes` | `BANK_MAX_LINE_BYTES` | – | `65536` |
| `client_idle_timeout` | `BANK_CLIENT_IDLE_TIMEOUT` | – | `300` s |
| `enable_fault_injection` | `BANK_ENABLE_FAULT_INJECTION=1` | `--enable-fault-injection` | `false` (debug only!) |

Relative paths are relative to the directory you start the server from (run it from the
project folder). Every request/response and every connect / disconnect / busy event is
written as one JSON line to stdout **and** `logs/server.log`, e.g.
```json
{"timestamp": "2026-10-21T15:35:25.245Z", "event": "request", "request_id": "cli-1a2b", "client_ip": "192.168.75.20", "client_port": 51544, "action": "withdraw", "account": "A1001", "result": "success", "balance": 970.0, "latency_ms": 3.1, ...}
```

## Running on two VMs

| VM | IP | role |
|---|---|---|
| server VM | `192.168.75.10` | runs `server.py` |
| client VM | `192.168.75.20` | runs `client.py` and the demos |

Copy the project folder to both VMs (e.g. `scp -r bank-assignment user@192.168.75.20:`).
Check Python: `python3 --version` (3.12 on Ubuntu 24.04, nothing to install).

**Server VM (192.168.75.10):**
```bash
cd bank-assignment
sudo ufw allow 5000/tcp          # only if the ufw firewall is enabled
python3 server.py                # listens on 0.0.0.0:5000, Ctrl+C (SIGINT) or SIGTERM stops it cleanly
```
Check that it listens: `ss -ltnp | grep 5000`.

**Client VM (192.168.75.20)** – always use the server's real IP, not `localhost`:
```bash
cd bank-assignment
ping -c1 192.168.75.10                        # network sanity check
python3 client.py --host 192.168.75.10        # interactive
#   bank> balance A1001
#   bank> deposit A1001 100
#   bank> withdraw A1001 30
#   bank> transfer A1001 A1002 50
#   bank> raw {"action":"fly","account":"A1001"}
#   bank> rawhex fffe00                        (arbitrary bytes)
#   bank> quit
python3 client.py --host 192.168.75.10 balance A1001     # one-shot (prints JSON, exit code 0/1)
export BANK_SERVER_HOST=192.168.75.10                    # then --host can be omitted
```
In the server log, `client_ip` must show `192.168.75.20`, which proves the request came over
the network.

## Demos (run on the client VM)

Start from a fresh database to get the exact numbers below: on the server VM stop the
server, `rm -f bank.db bank.db-wal bank.db-shm`, start it again.

| Demo | Command (client VM) | Shows |
|---|---|---|
| Race condition | `python3 demo/concurrent_withdrawals.py --host 192.168.75.10 --clients 50 --amount 30` | 50 simultaneous withdrawals of 30 from A1001 (1000): exactly **33 succeed, 17 INSUFFICIENT_FUNDS, final 10.00** |
| Error cases | `python3 demo/error_cases.py --host 192.168.75.10` | all error types incl. random bytes and a 70 KB line, connection still usable |
| Many clients | `python3 demo/multi_clients.py --host 192.168.75.10 --clients 8 --ops 25` | mixed concurrent ops; total money is conserved |
| Idempotency | `python3 demo/idempotency_demo.py --host 192.168.75.10` | same `request_id` sent twice -> applied once |
| MAX_CLIENTS | server: `python3 server.py --max-clients 3`; client: `python3 demo/max_clients_demo.py --host 192.168.75.10 --limit 3` | 4th connection gets `SERVER_BUSY` |
| Failure halfway | server: `BANK_ENABLE_FAULT_INJECTION=1 python3 server.py`; client: `python3 demo/fault_injection_demo.py --host 192.168.75.10` | crash between debit and credit -> `INTERNAL_ERROR`, balances unchanged |
| Durability | see below | committed data survives `kill -9` |

### Durability (kill -9) demo

1. Server VM: `python3 server.py`
2. Client VM: `python3 demo/durability_demo.py --host 192.168.75.10` – it makes 5 deposits of 100
   into A1003, prints the expected balance and waits.
3. Server VM: `kill -9 $(pgrep -f server.py)` (hard crash, no cleanup), then `python3 server.py` again.
4. Client VM: press Enter – the script reads the balance and checks every acknowledged deposit is there.

Manual version: `client.py ... deposit A1003 100` (note the balance) → `kill -9` → restart →
`client.py ... balance A1003` shows the same balance.
Single-machine version (starts and kills its own server): `python3 demo/durability_demo.py --local`.

## Tests

```bash
python3 -m unittest -v tests/test_bank.py
```
Each test class starts its own server process on a free port with a temporary database
(your `bank.db` is not touched). Covers: amount validation, config precedence,
50×30 concurrent withdrawals (33 successes, final 10), all error cases, fault-injection
rollback, idempotency, transfer conservation, client disconnect mid-request, MAX_CLIENTS,
kill -9 durability and graceful SIGTERM shutdown.
