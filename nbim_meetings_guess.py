"""

** nbim_meetings_guess.py **

Attempts to discover new meetings by guessing meeting IDs above (or below)
the highest (or lowest) known meeting ID in the database.

- UP (default): starts from the highest known ID + 1 and increments upward.
- DOWN: starts from the lowest known ID - 1 and decrements downward,
  skipping IDs already present in the database.

When a meeting is found it is saved to the `meetings` and `votes` tables,
and the corresponding company record in `companies` is updated with the
new meeting ID appended to its `meetings` field.

Operations are logged to nbim_meetings_guess.log.

Optional arguments:
    --limit N               Number of ID guesses to attempt (default: 10).
    --direction UP|DOWN     Direction to guess from the boundary ID (default: UP).
    --log OFF|STRICT|FULL   File logging level (default: STRICT).

"""

import argparse
import sys
from datetime import datetime

from mysql.connector import Error as MySQLError

from client.nbimvr_client import NBIMVRClient
from nbim_functions_db import connect_db, ensure_table, meeting_exists, insert_meeting, get_all_meeting_ids, upsert_company
from nbim_functions_shared import setup_logging, configure_file_logging, log_important


# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────

log = setup_logging("nbim_meetings_guess", "nbim_meetings_guess.log")


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
# Database helpers
# ──────────────────────────────────────────────

def get_boundary_meeting_id(conn, direction: str) -> int | None:
    """Return the highest (UP) or lowest (DOWN) meeting ID currently in the database."""
    cur = conn.cursor()
    if direction == "UP":
        cur.execute("SELECT MAX(id) FROM meetings;")
    else:
        cur.execute("SELECT MIN(id) FROM meetings;")
    result = cur.fetchone()
    cur.close()
    return result[0] if result and result[0] is not None else None


def append_meeting_to_company(conn, company_id: int, new_meeting_id: int, now: str) -> None:
    """Append a new meeting ID to the company's meetings field."""
    cur = conn.cursor()
    cur.execute(_GET_COMPANY_BY_ID_SQL, {"company_id": company_id})
    row = cur.fetchone()
    cur.close()

    if row is None:
        log.warning("WARNING company id=%s not found in database — cannot update meetings field.", company_id)
        return

    existing_meetings = row[1] or ""
    meeting_ids = [m for m in existing_meetings.split(",") if m.strip()] if existing_meetings else []
    if str(new_meeting_id) not in meeting_ids:
        meeting_ids.append(str(new_meeting_id))

    cur = conn.cursor()
    cur.execute(_UPDATE_COMPANY_MEETINGS_SQL, {
        "id":       company_id,
        "meetings": ",".join(meeting_ids),
        "updated":  now,
    })
    conn.commit()
    cur.close()


# ──────────────────────────────────────────────
# Argument parsing
# ──────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Discover new NBIM meetings by guessing IDs.")
    p.add_argument("--limit", type=int, default=10, metavar="N",
                   help="Number of ID guesses to attempt (default: 10).")
    p.add_argument("--direction", choices=["UP", "DOWN"], default="UP",
                   help="Direction to guess from the boundary ID: UP (default) or DOWN.")
    p.add_argument("--log", choices=["OFF", "STRICT", "FULL"], default="STRICT",
                   help="File logging level: OFF, STRICT (errors only, default), or FULL.")
    args = p.parse_args()
    if args.limit < 1:
        p.error("--limit must be a positive integer.")
    return args


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def run() -> None:
    args = parse_args()
    configure_file_logging(log, args.log)

    start_msg = "=== nbim_meetings_guess.py started at %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_msg += " --limit %d --direction %s" % (args.limit, args.direction)
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

    # Find boundary meeting ID
    try:
        boundary_id = get_boundary_meeting_id(conn, args.direction)
    except MySQLError as exc:
        log.error("Failed to determine boundary meeting ID: %s — aborting.", exc)
        conn.close()
        sys.exit(1)

    if boundary_id is None:
        log.error("No meeting IDs found in database — cannot determine starting point. Aborting.")
        conn.close()
        sys.exit(1)

    log.info("Boundary meeting ID (%s): %d. Will guess %d ID(s).", args.direction, boundary_id, args.limit)

    # Guess meeting IDs
    found_count = 0
    miss_count = 0
    skipped_count = 0
    error_count = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    step = 1 if args.direction == "UP" else -1

    for i in range(1, args.limit + 1):
        guess_id = boundary_id + (step * i)

        # For DOWN direction, skip IDs already in the database
        if args.direction == "DOWN":
            try:
                if meeting_exists(conn, guess_id):
                    log.debug("SKIP meeting id=%d — already exists in database.", guess_id)
                    skipped_count += 1
                    continue
            except MySQLError as exc:
                log.error("ERROR checking existence of meeting id=%d: %s", guess_id, exc)
                error_count += 1
                continue

        # Query the API
        try:
            meeting = client.get_meeting(guess_id)
        except Exception as exc:
            log.error("ERROR querying API for meeting id=%d: %s", guess_id, exc)
            error_count += 1
            continue

        if meeting is None:
            log.info("MISS meeting id=%d — not found.", guess_id)
            miss_count += 1
            continue

        # Save meeting to database
        try:
            insert_meeting(conn, meeting, now)
            log.info("FOUND meeting id=%-10d  %s  %s", meeting.id, meeting.company_name, meeting.date)
            found_count += 1
        except MySQLError as exc:
            log.error("ERROR could not write meeting id=%d to database: %s", guess_id, exc)
            conn.rollback()
            error_count += 1
            continue

        # Fetch and upsert the company the meeting belongs to
        if meeting.company_id:
            try:
                company = client.query_company_with_id(meeting.company_id)
                if company is None:
                    log.warning("WARNING could not fetch company id=%s from API — skipping company update.", meeting.company_id)
                else:
                    meeting_ids = []
                    if company.meetings:
                        for m in company.meetings:
                            if hasattr(m, "id") and m.id is not None:
                                meeting_ids.append(str(m.id))
                    row = {
                        "id":       company.id,
                        "name":     company.name,
                        "isin":     company.isin,
                        "ticker":   company.ticker,
                        "country":  company.country,
                        "meetings": ",".join(meeting_ids) if meeting_ids else None,
                        "updated":  now,
                    }
                    upsert_company(conn, row)
                    log.info("Upserted company id=%s  %s", company.id, company.name)
            except Exception as exc:
                log.error("ERROR updating company id=%s for meeting id=%d: %s",
                          meeting.company_id, meeting.id, exc)
                error_count += 1
        else:
            log.warning("WARNING meeting id=%d has no company_id — company not updated.", meeting.id)

    conn.close()
    log_important(log, "=== Finished. %d found, %d missed, %d skipped, %d error(s). ===" % (
        found_count, miss_count, skipped_count, error_count))


if __name__ == "__main__":
    run()