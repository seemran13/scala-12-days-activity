"""
Day 27 companion feeder - TCP server standing in for a bank's transaction
switch, streaming one CSV transaction line at a time to whichever Spark app
connects on port 9999 (same socketTextStream pattern as Days 23-26).

Account behavior, by design, to exercise every alert path in
BankTransactionStreamApp:

  ACC2001-ACC2010  "normal" accounts - make an ordinary transaction every
                                       few seconds through a mostly
                                       LOW/MEDIUM-risk branch. Never enough
                                       transactions in a 20s window to trip
                                       the burst threshold (6+).

  ACC2011          "bursty" account  - every ~50 seconds, fires 8-10
                                       transactions in rapid succession (a
                                       couple of seconds apart) through an
                                       ordinary branch - tuned to reliably
                                       trip the plain [BURST ALERT] without
                                       the HIGH-risk tag.

  ACC2012          "bursty + risky"  - same burst pattern as ACC2011, but
                   account             every burst transaction is routed
                                       through BR07 or BR08 (the two
                                       HIGH-risk branches in branches.csv) -
                                       tuned to trip [BURST ALERT] WITH the
                                       "(HIGH RISK BRANCH ACTIVITY)" tag.

Usage (Terminal 1, started BEFORE `sbt run` in Terminal 2):
    python3 scripts/txn_feed_server.py
"""

import random
import socket
import time

HOST = "localhost"
PORT = 9999

NORMAL_ACCOUNTS = [f"ACC{2000 + i}" for i in range(1, 11)]  # ACC2001-ACC2010
BURSTY_ACCOUNT = "ACC2011"
BURSTY_RISKY_ACCOUNT = "ACC2012"

NORMAL_BRANCHES = ["BR01", "BR02", "BR03", "BR04", "BR05", "BR06"]
HIGH_RISK_BRANCHES = ["BR07", "BR08"]

TXN_TYPES = ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT"]

BURST_INTERVAL_SEC = 50.0   # how often each bursty account fires a burst
BURST_SIZE_RANGE = (8, 10)  # transactions per burst
BURST_GAP_SEC = 1.5         # seconds between transactions WITHIN a burst

_txn_counter = 0


def next_txn_id():
    global _txn_counter
    _txn_counter += 1
    return f"TXN{_txn_counter:06d}"


def make_txn_line(account_id, branch_id):
    amount = round(random.uniform(200.0, 25000.0), 2)
    txn_type = random.choice(TXN_TYPES)
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")
    return f"{next_txn_id()},{account_id},{branch_id},{amount},{txn_type},{timestamp}"


def send_line(conn, line):
    conn.sendall((line + "\n").encode("utf-8"))


def serve(conn):
    print("[feeder] client connected - streaming transactions (Ctrl+C to stop)")
    last_burst_time = {BURSTY_ACCOUNT: 0.0, BURSTY_RISKY_ACCOUNT: 0.0}
    start_time = time.time()

    while True:
        now = time.time()

        # Ordinary trickle: one normal account makes one transaction every
        # ~2 seconds, through an ordinary branch.
        account_id = random.choice(NORMAL_ACCOUNTS)
        branch_id = random.choice(NORMAL_BRANCHES)
        send_line(conn, make_txn_line(account_id, branch_id))

        # occasional malformed/edge-case line, same spirit as earlier days'
        # seeded bad data, to exercise parseLine's validation path
        if random.random() < 0.04:
            send_line(conn, "BAD,ROW,MISSING,FIELDS")

        # Bursty accounts: once their interval has elapsed, fire a tight
        # cluster of transactions right now, then wait for the next interval.
        for account_id, branch_pool in (
            (BURSTY_ACCOUNT, NORMAL_BRANCHES),
            (BURSTY_RISKY_ACCOUNT, HIGH_RISK_BRANCHES),
        ):
            if now - last_burst_time[account_id] >= BURST_INTERVAL_SEC:
                burst_size = random.randint(*BURST_SIZE_RANGE)
                print(f"[feeder] {account_id} is firing a burst of {burst_size} transactions now")
                for _ in range(burst_size):
                    branch_id = random.choice(branch_pool)
                    send_line(conn, make_txn_line(account_id, branch_id))
                    time.sleep(BURST_GAP_SEC)
                last_burst_time[account_id] = time.time()

        time.sleep(2.0)


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(1)
    print(f"[feeder] listening on {HOST}:{PORT} - waiting for the Spark app to connect...")
    print(f"[feeder] {BURSTY_ACCOUNT} and {BURSTY_RISKY_ACCOUNT} will each burst roughly every "
          f"{int(BURST_INTERVAL_SEC)}s ({BURSTY_RISKY_ACCOUNT} through HIGH-risk branches)")

    try:
        while True:
            conn, addr = server.accept()
            try:
                serve(conn)
            except (BrokenPipeError, ConnectionResetError):
                print("[feeder] client disconnected")
            finally:
                conn.close()
            print("[feeder] waiting for a new connection...")
    except KeyboardInterrupt:
        print("\n[feeder] shutting down")
    finally:
        server.close()


if __name__ == "__main__":
    main()
