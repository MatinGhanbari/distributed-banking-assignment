"""Shared helper for the demo scripts: makes client.py importable."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from client import BankClient, add_server_args, require_host  # noqa: E402,F401
