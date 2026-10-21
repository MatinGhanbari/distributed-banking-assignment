#!/usr/bin/env bash
# Bonus 2: simulate network partitions on SERVER A (192.168.75.10) with iptables.
# Run as root on A:   sudo bash demo/vm_partition.sh cut-link | cut-all | heal | show
#
#   cut-link  drop ALL traffic between A and B (192.168.75.11) - both still reach the
#             witness on the client VM, clients on .20 still reach A:5000
#   cut-all   cut-link + drop A's traffic to/from the witness (192.168.75.20, TCP 7000 only);
#             clients on .20 can STILL reach A:5000 (A must answer NO_QUORUM)
#   heal      remove every rule this script added
#   show      list the rules
# All rules live in an own chain (BANKPART) so "heal" never touches other firewall rules.
# Override addresses with B_IP=... WITNESS_IP=... WITNESS_PORT=... if your VMs differ.
set -euo pipefail
B_IP=${B_IP:-192.168.75.11}
WITNESS_IP=${WITNESS_IP:-192.168.75.20}
WITNESS_PORT=${WITNESS_PORT:-7000}
CHAIN=BANKPART

ensure_chain() {
    iptables -N "$CHAIN" 2>/dev/null || true
    iptables -C INPUT -j "$CHAIN" 2>/dev/null || iptables -I INPUT 1 -j "$CHAIN"
    iptables -C OUTPUT -j "$CHAIN" 2>/dev/null || iptables -I OUTPUT 1 -j "$CHAIN"
}

case "${1:-}" in
  cut-link)
    ensure_chain
    iptables -F "$CHAIN"
    iptables -A "$CHAIN" -s "$B_IP" -j DROP                      # B -> A (anything)
    iptables -A "$CHAIN" -d "$B_IP" -j DROP                      # A -> B (anything)
    echo "A <-> B cut (witness and clients still reachable)" ;;
  cut-all)
    ensure_chain
    iptables -F "$CHAIN"
    iptables -A "$CHAIN" -s "$B_IP" -j DROP
    iptables -A "$CHAIN" -d "$B_IP" -j DROP
    iptables -A "$CHAIN" -d "$WITNESS_IP" -p tcp --dport "$WITNESS_PORT" -j DROP   # A -> witness
    iptables -A "$CHAIN" -s "$WITNESS_IP" -p tcp --sport "$WITNESS_PORT" -j DROP   # witness -> A
    echo "A isolated from B and from the witness (clients on $WITNESS_IP still reach A:5000)" ;;
  heal)
    iptables -D INPUT -j "$CHAIN" 2>/dev/null || true
    iptables -D OUTPUT -j "$CHAIN" 2>/dev/null || true
    iptables -F "$CHAIN" 2>/dev/null || true
    iptables -X "$CHAIN" 2>/dev/null || true
    echo "healed: all partition rules removed" ;;
  show)
    iptables -S "$CHAIN" 2>/dev/null || echo "no partition rules" ;;
  *)
    echo "usage: sudo bash $0 cut-link|cut-all|heal|show" >&2; exit 2 ;;
esac
