#!/usr/bin/env python3
"""
Day 25 companion script - a TCP server that generates a continuous, LIVE
stream of synthetic bank transactions for TransactionWindowApp
(ssc.socketTextStream) to read from, deliberately alternating between a
NORMAL phase and a BURST phase so the app's "sudden increase" detector
(section 5) actually has something real to catch.

Usage (run from the project root, e.g. ~/scala-spark-practice/day25-window-operations):
    python3 scripts/burst_txn_server.py
    (leave this running, then in a SECOND terminal: sbt run)

Stop with Ctrl+C once you've seen at least one full NORMAL -> BURST -> NORMAL
cycle (about a minute).

Timeline (repeats forever):
  - 45s NORMAL phase: one transaction every ~0.3-0.7s (~1.5-3 txn/s)
  - 15s BURST  phase: one transaction every ~0.03-0.08s (~15-30 txn/s)
  then back to NORMAL, and so on.
"""
import random
import socket
import time

HOST = "localhost"
PORT = 9999

ACCOUNTS = [f"ACC10{i:02d}" for i in range(1, 11)]
TYPES = ["DEBIT", "CREDIT"]

NORMAL_PHASE_SECONDS = 45
BURST_PHASE_SECONDS = 15


def make_line(seq: int) -> str:
    account = random.choice(ACCOUNTS)
    txn_type = random.choice(TYPES)
    amount = round(random.uniform(50, 25000), 2)
    ts = time.strftime("%Y-%m-%dT%H:%M:%S")

    # Same realistic failure modes as Day 24's feed - occasional missing
    # account or a corrupted amount field - so the app's parse/validate
    # step still has real work to do here, not just a clean synthetic feed.
    roll = random.random()
    if roll < 0.02:
        return f"T{seq:07d},{ts},,{txn_type},{amount}"
    if roll < 0.03:
        return f"T{seq:07d},{ts},{account},{txn_type},N/A"
    return f"T{seq:07d},{ts},{account},{txn_type},{amount}"


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(1)
    print(f"Listening on {HOST}:{PORT} - waiting for Spark to connect (start `sbt run` now)...")

    conn, addr = server.accept()
    print(f"Connected: {addr} - streaming transactions now (Ctrl+C here to stop)")
    print(f"Phase schedule: {NORMAL_PHASE_SECONDS}s NORMAL, then {BURST_PHASE_SECONDS}s BURST, repeating.\n")

    seq = 1
    phase_start = time.time()
    phase = "NORMAL"

    try:
        with conn:
            while True:
                elapsed = time.time() - phase_start
                if phase == "NORMAL" and elapsed >= NORMAL_PHASE_SECONDS:
                    phase = "BURST"
                    phase_start = time.time()
                    print(">>> entering BURST phase - transaction rate jumping up now")
                elif phase == "BURST" and elapsed >= BURST_PHASE_SECONDS:
                    phase = "NORMAL"
                    phase_start = time.time()
                    print(">>> back to NORMAL phase - transaction rate settling back down")

                line = make_line(seq)
                seq += 1
                conn.sendall((line + "\n").encode("utf-8"))

                if phase == "NORMAL":
                    time.sleep(random.uniform(0.3, 0.7))
                else:
                    time.sleep(random.uniform(0.03, 0.08))
    except (BrokenPipeError, ConnectionResetError):
        print("Spark disconnected.")
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        server.close()


if __name__ == "__main__":
    main()
