#!/usr/bin/env python3
"""
Day 24 companion script - a tiny TCP server that replays sample_transactions.txt
line by line over a socket, for BankTransactionStreamApp (ssc.socketTextStream)
to read from.

Usage (run from the project root, e.g. ~/scala-spark-practice/day24-stateless-vs-stateful):
    python3 scripts/txn_server.py
    (leave this running, then in a SECOND terminal: sbt run)

Stop with Ctrl+C once you've seen several batches and the running totals climb.
"""
import pathlib
import random
import socket
import time

HOST = "localhost"
PORT = 9999
TXN_FILE = pathlib.Path(__file__).resolve().parent.parent / "src" / "main" / "resources" / "sample_transactions.txt"


def main():
    # Keep blank lines in on purpose here (unlike Day 23's server) - they're
    # part of what the Scala app's stateless parse/filter step is meant to
    # exercise, same as the malformed rows already baked into the file.
    lines = TXN_FILE.read_text().splitlines()
    print(f"Loaded {len(lines)} transaction lines from {TXN_FILE.name}")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(1)
    print(f"Listening on {HOST}:{PORT} - waiting for Spark to connect (start `sbt run` now)...")

    conn, addr = server.accept()
    print(f"Connected: {addr} - streaming transactions now (Ctrl+C here to stop)")

    try:
        i = 0
        with conn:
            while True:
                line = lines[i % len(lines)]
                conn.sendall((line + "\n").encode("utf-8"))
                i += 1
                time.sleep(random.uniform(0.15, 0.7))
    except (BrokenPipeError, ConnectionResetError):
        print("Spark disconnected.")
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        server.close()


if __name__ == "__main__":
    main()
