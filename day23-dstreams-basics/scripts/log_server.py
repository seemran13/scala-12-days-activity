#!/usr/bin/env python3
"""
Day 23 companion script - a tiny TCP server that replays sample_app_logs.txt
line by line over a socket, for SocketLogStreamApp (ssc.socketTextStream) to
read from.

Usage (run from the project root, e.g. ~/scala-spark-practice/day23-dstreams-basics):
    python3 scripts/log_server.py
    (leave this running, then in a SECOND terminal: sbt run)

Stop with Ctrl+C once you've seen a few batches go by in the Spark terminal.
"""
import pathlib
import random
import socket
import time

HOST = "localhost"
PORT = 9999
LOG_FILE = pathlib.Path(__file__).resolve().parent.parent / "src" / "main" / "resources" / "sample_app_logs.txt"


def main():
    lines = [l for l in LOG_FILE.read_text().splitlines() if l.strip()]
    print(f"Loaded {len(lines)} log lines from {LOG_FILE.name}")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(1)
    print(f"Listening on {HOST}:{PORT} - waiting for Spark to connect (start `sbt run` now)...")

    conn, addr = server.accept()
    print(f"Connected: {addr} - streaming log lines now (Ctrl+C here to stop)")

    try:
        i = 0
        with conn:
            while True:
                line = lines[i % len(lines)]
                conn.sendall((line + "\n").encode("utf-8"))
                i += 1
                # Randomized small delay so several lines land within one
                # 5-second batch interval, but batches don't all look
                # identical - closer to a real, bursty log feed.
                time.sleep(random.uniform(0.15, 0.8))
    except (BrokenPipeError, ConnectionResetError):
        print("Spark disconnected.")
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        server.close()


if __name__ == "__main__":
    main()
