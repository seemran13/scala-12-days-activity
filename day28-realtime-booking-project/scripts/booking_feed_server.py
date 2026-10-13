"""
Day 28 companion feeder - TCP server standing in for a travel platform's
booking switch, streaming one CSV booking/cancellation event at a time to
whichever Spark app connects on port 9999 (same socketTextStream pattern as
Days 23-27).

Resource behavior, by design, to exercise every tier in BookingStreamApp:

  H1, H3, H4, F1, F3, B1, B2, B3   "normal" resources - a steady trickle of
                                   mostly BOOK events (occasional CANCEL),
                                   1-2 units each, never close to capacity.

  H2 ("Seaside Resort", cap=10)   deliberately booked hard and fast from
                                   the start - crosses NEARLY FULL (90%),
                                   then SOLD OUT (100%), then OVERBOOKED
                                   (>100%) within the first ~30 seconds,
                                   then gets a handful of cancellations
                                   later to show it partially recovering.
                                   Also the primary [TRENDING] resource.

  F2 ("AI305", cap=150)           booked up steadily to a healthy occupancy
                                   in the first ~40 seconds, then hit with a
                                   burst of CANCEL events all at once around
                                   the 50s mark (a group cancellation),
                                   showing occupancy drop sharply in one
                                   batch - exercises the cancellation half
                                   of the running state.

Usage (Terminal 1, started BEFORE `sbt run` in Terminal 2):
    python3 scripts/booking_feed_server.py
"""

import random
import socket
import time

HOST = "localhost"
PORT = 9999

NORMAL_RESOURCES = ["H1", "H3", "H4", "F1", "F3", "B1", "B2", "B3"]
HOT_HOTEL = "H2"       # small capacity, deliberately overbooked
BURST_CANCEL_FLIGHT = "F2"  # steady fill, then a cancellation wave

HOT_HOTEL_BOOK_UNTIL_SEC = 30.0      # H2 books hard for the first 30s
F2_FILL_UNTIL_SEC = 40.0             # F2 fills steadily for the first 40s
F2_CANCEL_BURST_AT_SEC = 50.0        # then a cancellation wave around 50s
F2_CANCEL_BURST_SIZE = 6

_event_counter = 0


def next_event_id():
    global _event_counter
    _event_counter += 1
    return f"EVT{_event_counter:06d}"


def make_line(resource_id, event_type, units, booking_id=None):
    booking_id = booking_id or f"BKG{random.randint(100000, 999999)}"
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")
    return f"{next_event_id()},{booking_id},{resource_id},{event_type},{units},{timestamp}"


def send_line(conn, line):
    conn.sendall((line + "\n").encode("utf-8"))


def serve(conn):
    print("[feeder] client connected - streaming booking events (Ctrl+C to stop)")
    start_time = time.time()
    f2_cancel_fired = False

    while True:
        elapsed = time.time() - start_time

        # Normal trickle: one or two ordinary resources get an ordinary
        # event roughly every ~2 seconds.
        for _ in range(random.randint(1, 2)):
            resource_id = random.choice(NORMAL_RESOURCES)
            event_type = "BOOK" if random.random() < 0.8 else "CANCEL"
            units = random.randint(1, 2)
            send_line(conn, make_line(resource_id, event_type, units))

        # occasional malformed/edge-case line, same spirit as earlier days
        if random.random() < 0.04:
            send_line(conn, "BAD,ROW,MISSING")

        # H2: book hard and fast for the first 30s (drives it past capacity
        # and keeps it trending), then settle into light, mostly-CANCEL
        # traffic so it partially recovers.
        if elapsed < HOT_HOTEL_BOOK_UNTIL_SEC:
            for _ in range(random.randint(1, 2)):
                send_line(conn, make_line(HOT_HOTEL, "BOOK", random.randint(1, 2)))
        elif random.random() < 0.5:
            event_type = "CANCEL" if random.random() < 0.7 else "BOOK"
            send_line(conn, make_line(HOT_HOTEL, event_type, 1))

        # F2: fill steadily for the first 40s, then fire one concentrated
        # cancellation burst around the 50s mark, then go back to a normal
        # trickle.
        if elapsed < F2_FILL_UNTIL_SEC:
            if random.random() < 0.6:
                send_line(conn, make_line(BURST_CANCEL_FLIGHT, "BOOK", random.randint(2, 4)))
        elif elapsed >= F2_CANCEL_BURST_AT_SEC and not f2_cancel_fired:
            print(f"[feeder] {BURST_CANCEL_FLIGHT} is firing a cancellation burst now")
            for _ in range(F2_CANCEL_BURST_SIZE):
                send_line(conn, make_line(BURST_CANCEL_FLIGHT, "CANCEL", random.randint(2, 5)))
            f2_cancel_fired = True
        elif random.random() < 0.3:
            send_line(conn, make_line(BURST_CANCEL_FLIGHT, "BOOK", random.randint(1, 2)))

        time.sleep(2.0)


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(1)
    print(f"[feeder] listening on {HOST}:{PORT} - waiting for the Spark app to connect...")
    print(f"[feeder] {HOT_HOTEL} fills past capacity in the first ~{int(HOT_HOTEL_BOOK_UNTIL_SEC)}s; "
          f"{BURST_CANCEL_FLIGHT} gets a cancellation burst around ~{int(F2_CANCEL_BURST_AT_SEC)}s")

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
