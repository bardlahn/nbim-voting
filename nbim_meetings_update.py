"""

** nbim_meetings_update.py **

Reads all meeting IDs from the `companies` table in the `nbim_data` MySQL
database, fetches each meeting from the NBIM API, and writes the data to
the `meetings` and `votes` tables.

Only adds meetings that do not already exist in the database.

Operations are logged to nbim_meetings_update.log.

Reads database credentials from client/secrets.txt:
    DB_USER=<username>
    DB_SECRET=<password>

Optional arguments:
    --limit N               Stop after fetching N meetings.
    --log OFF|STRICT|FULL   File logging level (default: STRICT).
                            OFF    = no logging to file.
                            STRICT = only start, end, and error messages.
                            FULL   = all messages.

"""

import argparse
import sys
from datetime import datetime

from mysql.connector import Error as MySQLError

from client.nbimvr_client import NBIMVRClient
from nbim_functions_db import connect_db, ensure_table, meeting_exists, insert_meeting, get_all_meeting_ids
from nbim_functions_shared import setup_logging, configure_file_logging, log_important


# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────

log = setup_logging("nbim_meetings_update", "nbim_meetings_update.log")


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
# Argument parsing
# ──────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Populate the meetings and votes tables from the NBIM API.")
    p.add_argument("--limit", type=int, default=None, metavar="N",
                   help="Stop after fetching N meetings.")
    p.add_argument("--log", choices=["OFF", "STRICT", "FULL"], default="STRICT",
                   help="File logging level: OFF, STRICT (errors only, default), or FULL.")
    args = p.parse_args()
    if args.limit is not None and args.limit < 1:
        p.error("--limit must be a positive integer.")
    return args


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def run() -> None:
    args = parse_args()
    configure_file_logging(log, args.log)

    start_msg = "=== nbim_meetings_update.py started at %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if args.limit:
        start_msg += " --limit %d" % args.limit
    if args.log != "STRICT":
        start_msg += " --log %s" % args.log
    start_msg += " ==="
    log_important(log, start_msg)

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
    log_important(log, "=== Finished. %d meeting(s) added, %d skipped, %d error(s). ===" % (
        success_count, skipped_count, error_count))


if __name__ == "__main__":
    run()
