#!/usr/bin/env python3
"""
Day 23 companion script - periodically drops a new small log file into
streaming_input/, for FileLogStreamApp (ssc.textFileStream) to pick up.

Usage (run from the project root):
    sbt "runMain day23.FileLogStreamApp"      (in one terminal, leave running)
    python3 scripts/file_stream_feeder.py     (in a second terminal)

Stop with Ctrl+C once you've seen a few batches go by in the Spark terminal.
"""
import pathlib
import random
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
LOG_FILE = ROOT / "src" / "main" / "resources" / "sample_app_logs.txt"
WATCH_DIR = ROOT / "streaming_input"


def main():
    WATCH_DIR.mkdir(exist_ok=True)
    lines = [l for l in LOG_FILE.read_text().splitlines() if l.strip()]
    print(f"Loaded {len(lines)} candidate log lines. Dropping a new file into {WATCH_DIR} every ~4 seconds.")
    print("Stop with Ctrl+C.")

    batch_num = 1
    try:
        while True:
            sample_size = random.randint(5, 15)
            sample = random.sample(lines, sample_size)
            out_file = WATCH_DIR / f"batch_{batch_num:04d}.txt"
            # Write to a .tmp name first, then rename into place. textFileStream
            # expects each file to appear as a complete, atomic unit - writing
            # directly to the watched name risks Spark reading a half-written
            # file mid-batch-scan.
            tmp_file = out_file.with_suffix(".tmp")
            tmp_file.write_text("\n".join(sample) + "\n")
            tmp_file.rename(out_file)
            print(f"  wrote {out_file.name} ({sample_size} lines)")
            batch_num += 1
            time.sleep(4)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
