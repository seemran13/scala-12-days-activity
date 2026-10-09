"""
Day 26 companion feeder - TCP server that plays the role of a hospital's
bedside monitors, streaming one CSV vital-sign reading per line to whichever
Spark app connects on port 9999 (same socketTextStream pattern as Days 23-25).

Patient behavior, by design, to exercise every alert path in VitalStreamApp:

  PAT001-PAT009  "normal" patients   - vitals wobble gently inside the
                                       healthy range almost all the time,
                                       with only the occasional one-off blip
                                       (not enough to ever hit a 3-in-a-row
                                       streak or 5-in-a-window count).

  PAT010, PAT011 "unstable" patients - abnormal readings fire often but not
                                       consistently (~35% of their readings),
                                       scattered rather than consecutive.
                                       This is tuned to realistically trip
                                       the WINDOWED frequency detector
                                       (5+ abnormal in 20s) without reliably
                                       tripping the STATEFUL consecutive
                                       streak detector (3 in a row) - the
                                       scenario the README calls out as
                                       "unstable but not strictly worsening".

  PAT012         "crashing" patient  - normal for the first ~60 seconds,
                                       then every vital goes and STAYS
                                       abnormal (high heart rate, low spo2,
                                       high BP, fever) for about 40 seconds
                                       straight, then recovers back to
                                       normal. This is tuned to reliably
                                       trip BOTH the stateful 3-in-a-row
                                       streak AND the windowed 5-in-20s
                                       count, since it's a sustained run of
                                       abnormal readings, not scattered ones.

One reading per patient is emitted every ~2 seconds (12 patients -> a steady
drip of lines, comfortably inside Spark's Seconds(2) batch interval), so the
client sees a continuous, realistic multi-patient ward feed rather than a
burst.

Usage (Terminal 1, started BEFORE `sbt run` in Terminal 2):
    python3 scripts/vital_feed_server.py
"""

import random
import socket
import time

HOST = "localhost"
PORT = 9999
SECONDS_PER_ROUND = 2.0  # one round = one reading per patient

PATIENTS = [f"PAT{str(i).zfill(3)}" for i in range(1, 13)]
UNSTABLE_PATIENTS = {"PAT010", "PAT011"}
CRASHING_PATIENT = "PAT012"

CRASH_START_SEC = 60.0   # crash begins ~60s into the feed
CRASH_DURATION_SEC = 40.0  # stays abnormal for ~40s, then recovers

# Normal (healthy) ranges, mirrored from VitalStreamApp's thresholds map -
# "normal" sampling draws comfortably inside these so false alarms on
# PAT001-PAT009 stay rare; "abnormal" sampling draws clearly outside them.
NORMAL_RANGES = {
    "heartRate": (62.0, 95.0),
    "spo2": (96.0, 100.0),
    "systolicBP": (95.0, 135.0),
    "diastolicBP": (62.0, 88.0),
    "temperature": (36.3, 37.6),
}
ABNORMAL_RANGES = {
    "heartRate": (115.0, 160.0),   # tachycardia
    "spo2": (82.0, 93.0),          # desaturation
    "systolicBP": (150.0, 185.0),  # hypertensive
    "diastolicBP": (92.0, 110.0),
    "temperature": (38.2, 40.1),   # fever
}


def sample(ranges):
    return {name: round(random.uniform(lo, hi), 1) for name, (lo, hi) in ranges.items()}


def make_reading(patient_id, abnormal):
    vitals = sample(ABNORMAL_RANGES if abnormal else NORMAL_RANGES)
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")
    return (
        f"{patient_id},{timestamp},{vitals['heartRate']},{vitals['spo2']},"
        f"{vitals['systolicBP']},{vitals['diastolicBP']},{vitals['temperature']}"
    )


def is_abnormal_this_round(patient_id, elapsed_sec):
    if patient_id == CRASHING_PATIENT:
        return CRASH_START_SEC <= elapsed_sec < (CRASH_START_SEC + CRASH_DURATION_SEC)
    if patient_id in UNSTABLE_PATIENTS:
        return random.random() < 0.35
    # normal patients: a rare one-off blip, never enough to chain into an alert
    return random.random() < 0.03


def serve(conn, start_time):
    print("[feeder] client connected - streaming vitals (Ctrl+C to stop)")
    while True:
        elapsed = time.time() - start_time
        lines = []
        for patient_id in PATIENTS:
            abnormal = is_abnormal_this_round(patient_id, elapsed)
            lines.append(make_reading(patient_id, abnormal))
        # occasional malformed/edge-case line, same spirit as Days 23-25's
        # seeded bad data, to exercise parseLine's validation path
        if random.random() < 0.04:
            lines.append("MALFORMED,ROW,MISSING,FIELDS")
        random.shuffle(lines)
        payload = "\n".join(lines) + "\n"
        conn.sendall(payload.encode("utf-8"))
        time.sleep(SECONDS_PER_ROUND)


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(1)
    print(f"[feeder] listening on {HOST}:{PORT} - waiting for the Spark app to connect...")
    print(f"[feeder] PAT012 will start crashing ~{int(CRASH_START_SEC)}s after a client connects, "
          f"for ~{int(CRASH_DURATION_SEC)}s")

    try:
        while True:
            conn, addr = server.accept()
            start_time = time.time()
            try:
                serve(conn, start_time)
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
