"""
Day 30 companion feeder - TCP server standing in for an insurer's claims
intake system, streaming one CSV claim submission at a time to whichever
Spark app connects on port 9999 (same socketTextStream pattern as Days
23-28).

Reads policies.csv (generated alongside this feeder for the batch app) so
it knows which policyIds belong to the normal population vs the seeded
"fraud ring" customer, CUST0001.

Behavior, by design:

  Normal policies    a steady trickle of ordinary claims, random policy,
                      random amount, rarely more than 1-2 claims per
                      customer in any 20s window - never trips the burst
                      detector.

  CUST0001's 7        every ~45 seconds, fires 5-7 claims across its OWN
  policies            several policies in rapid succession (a couple of
                      seconds apart) - enough that the customer-level
                      burst detector (which resolves policyId -> customerId
                      via the broadcast lookup before counting) reliably
                      fires, even though no single POLICY sees that many
                      claims on its own.

Usage (Terminal 1, started BEFORE `sbt run` in Terminal 2):
    python3 scripts/claims_feed_server.py
"""

import csv
import random
import socket
import time

HOST = "localhost"
PORT = 9999
POLICIES_PATH = "src/main/resources/policies.csv"

FRAUD_RING_CUSTOMER = "CUST0001"
CLAIM_TYPES = ["COLLISION", "THEFT", "FIRE", "MEDICAL", "NATURAL_DISASTER", "OTHER"]

BURST_INTERVAL_SEC = 45.0
BURST_SIZE_RANGE = (5, 7)
BURST_GAP_SEC = 1.5

_claim_counter = 0


def next_claim_id():
    global _claim_counter
    _claim_counter += 1
    return f"LIVE{_claim_counter:06d}"


def load_policies():
    normal_policy_ids = []
    fraud_ring_policy_ids = []
    with open(POLICIES_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["customerId"] == FRAUD_RING_CUSTOMER:
                fraud_ring_policy_ids.append(row["policyId"])
            else:
                normal_policy_ids.append(row["policyId"])
    return normal_policy_ids, fraud_ring_policy_ids


def make_line(policy_id):
    amount = round(random.uniform(2000.0, 90000.0), 2)
    claim_type = random.choice(CLAIM_TYPES)
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")
    return f"{next_claim_id()},{policy_id},{amount},{claim_type},{timestamp}"


def send_line(conn, line):
    conn.sendall((line + "\n").encode("utf-8"))


def serve(conn, normal_policy_ids, fraud_ring_policy_ids):
    print("[feeder] client connected - streaming claims (Ctrl+C to stop)")
    last_burst_time = 0.0

    while True:
        now = time.time()

        # Ordinary trickle
        policy_id = random.choice(normal_policy_ids)
        send_line(conn, make_line(policy_id))

        if random.random() < 0.04:
            send_line(conn, "BAD,ROW,MISSING")

        # Fraud-ring burst
        if now - last_burst_time >= BURST_INTERVAL_SEC:
            burst_size = random.randint(*BURST_SIZE_RANGE)
            print(f"[feeder] {FRAUD_RING_CUSTOMER} is firing a burst of {burst_size} claims now")
            for _ in range(burst_size):
                policy_id = random.choice(fraud_ring_policy_ids)
                send_line(conn, make_line(policy_id))
                time.sleep(BURST_GAP_SEC)
            last_burst_time = time.time()

        time.sleep(2.0)


def main():
    normal_policy_ids, fraud_ring_policy_ids = load_policies()
    if not fraud_ring_policy_ids:
        raise RuntimeError(f"No policies found for {FRAUD_RING_CUSTOMER} in {POLICIES_PATH}")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(1)
    print(f"[feeder] listening on {HOST}:{PORT} - waiting for the Spark app to connect...")
    print(f"[feeder] {FRAUD_RING_CUSTOMER} ({len(fraud_ring_policy_ids)} policies) will burst roughly every "
          f"{int(BURST_INTERVAL_SEC)}s")

    try:
        while True:
            conn, addr = server.accept()
            try:
                serve(conn, normal_policy_ids, fraud_ring_policy_ids)
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
