"""

** meetinglist.py **

Reads all meeting IDs from the `meetings` field in the `companies` table,
deduplicates and sorts them, and writes the result to meetinglist.tmp
(one ID per line).

Reads database credentials from client/secrets.txt:
    DB_USER=<username>
    DB_SECRET=<password>

"""

import sys

import mysql.connector
from mysql.connector import Error as MySQLError

from client.nbimvr_client import NBIMVRClient  # noqa: F401 — not used here, but keeps import consistent


# ──────────────────────────────────────────────
# Secrets
# ──────────────────────────────────────────────

def load_secrets(path: str = "client/secrets.txt") -> dict:
    secrets = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, _, value = line.partition("=")
            secrets[key.strip()] = value.strip()
    return secrets


# ──────────────────────────────────────────────
# Database
# ──────────────────────────────────────────────

GET_MEETINGS_SQL = "SELECT meetings FROM companies WHERE meetings IS NOT NULL AND meetings != '';"


def get_connection(user: str, password: str):
    return mysql.connector.connect(
        host="localhost",
        port=3306,
        database="nbim_data",
        user=user,
        password=password,
        charset="utf8mb4",
        autocommit=False,
    )


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def run() -> None:
    # Load credentials
    try:
        secrets = load_secrets("client/secrets.txt")
        db_user = secrets["DB_USER"]
        db_password = secrets["DB_SECRET"]
    except FileNotFoundError:
        print("ERROR: Could not find client/secrets.txt — aborting.")
        sys.exit(1)
    except KeyError as exc:
        print("ERROR: Missing key in client/secrets.txt: %s — aborting." % exc)
        sys.exit(1)

    # Connect to database
    try:
        conn = get_connection(db_user, db_password)
    except MySQLError as exc:
        print("ERROR: Could not connect to database: %s — aborting." % exc)
        sys.exit(1)

    # Fetch all meeting ID strings from companies table
    try:
        cur = conn.cursor()
        cur.execute(GET_MEETINGS_SQL)
        rows = cur.fetchall()
        cur.close()
    except MySQLError as exc:
        print("ERROR: Could not query companies table: %s — aborting." % exc)
        conn.close()
        sys.exit(1)

    conn.close()

    # Parse, deduplicate and sort meeting IDs
    meeting_ids = set()
    for (meetings_str,) in rows:
        for mid in meetings_str.split(","):
            mid = mid.strip()
            if mid.isdigit():
                meeting_ids.add(int(mid))

    sorted_ids = sorted(meeting_ids)
    print("Found %d unique meeting ID(s)." % len(sorted_ids))

    # Write to meetinglist.tmp
    output_path = "meetinglist.tmp"
    with open(output_path, "w", encoding="utf-8") as f:
        for mid in sorted_ids:
            f.write("%d\n" % mid)

    print("Written to %s." % output_path)

    # Interval analysis (based on last 100,000 IDs only)
    analysis_ids = sorted_ids[-100000:]
    if len(analysis_ids) >= 2:
        intervals = [analysis_ids[i + 1] - analysis_ids[i] for i in range(len(analysis_ids) - 1)]
        avg_interval = sum(intervals) / len(intervals)
        sorted_intervals = sorted(intervals)
        mid = len(sorted_intervals) // 2
        median_interval = (sorted_intervals[mid] if len(sorted_intervals) % 2 != 0
                           else (sorted_intervals[mid - 1] + sorted_intervals[mid]) / 2)
        max_interval = max(intervals)
        max_interval_idx = intervals.index(max_interval)
        max_interval_from = analysis_ids[max_interval_idx]
        max_interval_to = analysis_ids[max_interval_idx + 1]

        print("Interval analysis based on last %d IDs (from ID %d onwards)." % (len(analysis_ids), analysis_ids[0]))
        print("Average interval between consecutive meeting IDs: %.2f" % avg_interval)
        print("Median interval between consecutive meeting IDs:  %.2f" % median_interval)
        print("Longest interval: %d (between ID %d and %d)" % (max_interval, max_interval_from, max_interval_to))
    else:
        print("Not enough meeting IDs for interval analysis.")


if __name__ == "__main__":
    run()