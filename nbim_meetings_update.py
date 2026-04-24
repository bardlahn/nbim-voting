"""

** update_meetings.py **

Reads all meeting IDs from the `companies` table in the `nbim_data` MySQL
database, fetches each meeting from the NBIM API, and writes the data to
the `meetings` and `votes` tables.

Only adds meetings that do not already exist in the database.

Operations are logged to update_meetings.log.

Optional arguments:
    --limit N               Stop after fetching N meetings.
    --log OFF|STRICT|FULL   File logging level (default: FULL).
                            OFF    = no logging to file.
                            STRICT = only start, end, and error messages.
                            FULL   = all messages (default).

"""

import argparse
import logging
import sys
from datetime import datetime

from mysql.connector import Error as MySQLError

from client.nbimvr_client import NBIMVRClient
from nbim_db_functions import connect_db, ensure_table, meeting_exists, insert_meeting


# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────

log = logging.getLogger("update_meetings")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler("update_meetings.log", encoding="utf-8")
file_handler.setFormatter(formatter)


def configure_file_logging(level: str) -> None:
    if level == "OFF":
        return
    if level == "STRICT":
        file_handler.setLevel(logging.ERROR)
    else:
        file_handler.setLevel(logging.DEBUG)
    log.addHandler(file_handler)


def log_important(msg: str) -> None:
    log.info(msg)
    if file_handler in log.handlers and file_handler.level > logging.INFO:
        record = log.makeRecord(log.name, logging.INFO, "", 0, msg, (), None)
        file_handler.emit(record)


# ──────────────────────────────────────────────
# DDL
# ──────────────────────────────────────────────

DDL_MEETINGS = """
CREATE TABLE IF NOT EXISTS meetings (
    id              INT             PRIMARY KEY,
    type            VARCHAR(64),
    date            DATE,
    company_id      INT,
    company_name    VARCHAR(512),
    company_ticker  VARCHAR(64),
    isin            VARCHAR(32),
    updated         DATETIME,
    INDEX idx_company_id    (company_id),
    INDEX idx_isin          (isin),
    INDEX idx_date          (date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_VOTES = """
CREATE TABLE IF NOT EXISTS votes (
    id                  INT             PRIMARY KEY AUTO_INCREMENT,
    item_on_agenda_id   INT,
    meeting_id          INT,
    proposal_number     VARCHAR(32),
    proposal_sequence   INT,
    proposal_text       TEXT,
    proponent           VARCHAR(64),
    management_rec      VARCHAR(32),
    vote_instruction    VARCHAR(32),
    voter_rationale     JSON,
    INDEX idx_meeting_id        (meeting_id),
    INDEX idx_item_on_agenda_id (item_on_agenda_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# ──────────────────────────────────────────────
# Database helpers local to this script
# ──────────────────────────────────────────────

_GET_ALL_MEETINGS_SQL = "SELECT meetings FROM companies WHERE meetings IS NOT NULL AND meetings != '';"


def get_all_meeting_ids(conn) -> list[int]:
    cur = conn.cursor()
    cur.execute(_GET_ALL_MEETINGS_SQL)
    rows = cur.fetchall()
    cur.close()
    meeting_ids = []
    for (meetings_str,) in rows:
        for mid in meetings_str.split(","):
            mid = mid.strip()
            if mid.isdigit():
                meeting_ids.append(int(mid))
    seen = set()
    unique_ids = []
    for mid in meeting_ids:
        if mid not in seen:
            seen.add(mid)
            unique_ids.append(mid)
    return unique_ids


# ──────────────────────────────────────────────
# Argument parsing
# ──────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Populate the meetings and votes tables from the NBIM API.")
    p.add_argument("--limit", type=int, default=None, metavar="N",
                   help="Stop after fetching N meetings.")
    p.add_argument("--log", choices=["OFF", "STRICT", "FULL"], default="FULL",
                   help="File logging level: OFF, STRICT (errors only), or FULL (default).")
    args = p.parse_args()
    if args.limit is not None and args.limit < 1:
        p.error("--limit must be a positive integer.")
    return args


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def run() -> None:
    args = parse_args()
    configure_file_logging(args.log)

    start_msg = "=== update_meetings.py started at %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if args.limit:
        start_msg += " --limit %d" % args.limit
    if args.log != "FULL":
        start_msg += " --log %s" % args.log
    start_msg += " ==="
    log_important(start_msg)

    # Connect to database
    try:
        conn = connect_db()
        ensure_table(conn, DDL_MEETINGS)
        ensure_table(conn, DDL_VOTES)
        log.info("Connected to database `nbim_data`.")
    except (MySQLError, KeyError, FileNotFoundError) as exc:
        log.error("Could not connect to database: %s — aborting.", exc)
        sys.exit(1)

    # Initialise API client
    try:
        client = NBIMVRClient()
        log.info("NBIMVRClient initialised.")
    except Exception as exc:
        log.error("Could not initialise NBIMVRClient: %s — aborting.", exc)
        conn.close()
        sys.exit(1)

    # Collect all meeting IDs from companies table
    try:
        meeting_ids = get_all_meeting_ids(conn)
        log.info("Found %d unique meeting ID(s) across all companies.", len(meeting_ids))
    except MySQLError as exc:
        log.error("Failed to read meeting IDs from companies table: %s — aborting.", exc)
        conn.close()
        sys.exit(1)

    # Process each meeting
    success_count = 0
    skipped_count = 0
    error_count = 0
    fetch_count = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for meeting_id in meeting_ids:
        # Skip if already in database
        try:
            if meeting_exists(conn, meeting_id):
                log.debug("SKIP meeting id=%s — already exists in database.", meeting_id)
                skipped_count += 1
                continue
        except MySQLError as exc:
            log.error("ERROR checking existence of meeting id=%s: %s", meeting_id, exc)
            error_count += 1
            continue

        # Stop if API fetch limit has been reached
        if args.limit and fetch_count >= args.limit:
            log.info("API fetch limit of %d reached — stopping.", args.limit)
            break

        # Fetch meeting from API
        try:
            meeting = client.get_meeting(meeting_id)
            fetch_count += 1
        except Exception as exc:
            log.error("ERROR fetching meeting id=%s from API: %s", meeting_id, exc)
            error_count += 1
            continue

        if meeting is None:
            log.error("ERROR no data returned for meeting id=%s.", meeting_id)
            error_count += 1
            continue

        # Write to database
        try:
            vote_count = len(meeting.votes) if meeting.votes else 0
            insert_meeting(conn, meeting, now)
            log.info("OK  meeting id=%-10s  %s  %s  (%d vote(s))",
                     meeting.id, meeting.company_name, meeting.date, vote_count)
            success_count += 1
        except MySQLError as exc:
            log.error("ERROR could not write meeting id=%s to database: %s", meeting_id, exc)
            conn.rollback()
            error_count += 1

    conn.close()
    log_important("=== Finished. %d meeting(s) added, %d skipped, %d error(s). ===" % (
        success_count, skipped_count, error_count))


if __name__ == "__main__":
    run()