"""

** update_meetings.py **

Reads all meeting IDs from the `companies` table in the `nbim_data` MySQL
database, fetches each meeting from the NBIM API, and writes the data to
the `meetings` and `votes` tables.

Only adds meetings that do not already exist in the database.

Operations are logged to update_meetings.log.

Reads database credentials from client/secrets.txt:
    DB_USER=<username>
    DB_SECRET=<password>

Optional arguments:
    --limit N       Stop after fetching N meetings.

"""

import argparse
import json
import logging
import sys
from datetime import datetime

import mysql.connector
from mysql.connector import Error as MySQLError

from client.nbimvr_client import NBIMVRClient


# Setting up logging

log = logging.getLogger("update_meetings")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

file_handler = logging.FileHandler("update_meetings.log", encoding="utf-8")
file_handler.setFormatter(formatter)
log.addHandler(file_handler)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)


# Fetching secrets

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


# Setting up database commands

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

INSERT_MEETING_SQL = """
INSERT INTO meetings (id, type, date, company_id, company_name, company_ticker, isin, updated)
VALUES (%(id)s, %(type)s, %(date)s, %(company_id)s, %(company_name)s, %(company_ticker)s, %(isin)s, %(updated)s);
"""

INSERT_VOTE_SQL = """
INSERT INTO votes
    (item_on_agenda_id, meeting_id, proposal_number, proposal_sequence,
     proposal_text, proponent, management_rec, vote_instruction, voter_rationale)
VALUES
    (%(item_on_agenda_id)s, %(meeting_id)s, %(proposal_number)s, %(proposal_sequence)s,
     %(proposal_text)s, %(proponent)s, %(management_rec)s, %(vote_instruction)s, %(voter_rationale)s);
"""

MEETING_EXISTS_SQL  = "SELECT 1 FROM meetings WHERE id = %(id)s LIMIT 1;"
GET_ALL_MEETINGS_SQL = "SELECT meetings FROM companies WHERE meetings IS NOT NULL AND meetings != '';"


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


def ensure_tables(conn) -> None:
    cur = conn.cursor()
    cur.execute(DDL_MEETINGS)
    cur.execute(DDL_VOTES)
    conn.commit()
    cur.close()


def meeting_exists(conn, meeting_id: int) -> bool:
    cur = conn.cursor()
    cur.execute(MEETING_EXISTS_SQL, {"id": meeting_id})
    result = cur.fetchone()
    cur.close()
    return result is not None


def get_all_meeting_ids(conn) -> list[int]:
    cur = conn.cursor()
    cur.execute(GET_ALL_MEETINGS_SQL)
    rows = cur.fetchall()
    cur.close()
    meeting_ids = []
    for (meetings_str,) in rows:
        for mid in meetings_str.split(","):
            mid = mid.strip()
            if mid.isdigit():
                meeting_ids.append(int(mid))
    # Deduplicate while preserving order
    seen = set()
    unique_ids = []
    for mid in meeting_ids:
        if mid not in seen:
            seen.add(mid)
            unique_ids.append(mid)
    return unique_ids


def insert_meeting(conn, meeting, now: str) -> None:
    cur = conn.cursor()

    # Parse date — strip time component if present
    date_str = meeting.date
    if date_str and " " in date_str:
        date_str = date_str.split(" ")[0]

    meeting_row = {
        "id":             meeting.id,
        "type":           meeting.type,
        "date":           date_str,
        "company_id":     meeting.company_id,
        "company_name":   meeting.company_name,
        "company_ticker": meeting.company_ticker,
        "isin":           meeting.isin,
        "updated":        now,
    }
    cur.execute(INSERT_MEETING_SQL, meeting_row)

    # Insert all votes for this meeting
    for vote in (meeting.votes or []):
        rationale = vote.voter_rationale
        vote_row = {
            "item_on_agenda_id": vote.item_on_agenda_id,
            "meeting_id":        meeting.id,
            "proposal_number":   vote.proposal_number,
            "proposal_sequence": vote.proposal_sequence,
            "proposal_text":     vote.proposal_text,
            "proponent":         vote.proponent,
            "management_rec":    vote.management_rec,
            "vote_instruction":  vote.vote_instruction,
            "voter_rationale":   json.dumps(rationale) if rationale is not None else None,
        }
        cur.execute(INSERT_VOTE_SQL, vote_row)

    conn.commit()
    cur.close()


# Argument parsing

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Populate the meetings and votes tables from the NBIM API.")
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Stop after fetching N meetings.",
    )
    args = p.parse_args()
    if args.limit is not None and args.limit < 1:
        p.error("--limit must be a positive integer.")
    return args


# Main functionality

def run() -> None:
    args = parse_args()

    start_msg = "=== update_meetings.py started at %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if args.limit:
        start_msg += " --limit %d" % args.limit
    start_msg += " ==="
    log.info(start_msg)

    # Load credentials
    try:
        secrets = load_secrets("client/secrets.txt")
        db_user = secrets["DB_USER"]
        db_password = secrets["DB_SECRET"]
    except FileNotFoundError:
        log.error("Could not find client/secrets.txt — aborting.")
        sys.exit(1)
    except KeyError as exc:
        log.error("Missing key in client/secrets.txt: %s — aborting.", exc)
        sys.exit(1)

    # Connect to database
    try:
        conn = get_connection(db_user, db_password)
        ensure_tables(conn)
        log.info("Connected to database `nbim_data`.")
    except MySQLError as exc:
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

    # Apply limit if provided
    if args.limit:
        meeting_ids = meeting_ids[:args.limit]
        log.info("Limiting to %d meeting(s).", len(meeting_ids))

    # Process each meeting
    success_count = 0
    skipped_count = 0
    error_count = 0
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

        # Fetch meeting from API
        try:
            meeting = client.get_meeting(meeting_id)
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
            log.info(
                "OK  meeting id=%-10s  %s  %s  (%d vote(s))",
                meeting.id,
                meeting.company_name,
                meeting.date,
                vote_count,
            )
            success_count += 1
        except MySQLError as exc:
            log.error(
                "ERROR could not write meeting id=%s to database: %s",
                meeting_id, exc,
            )
            conn.rollback()
            error_count += 1

    conn.close()
    log.info(
        "=== Finished. %d meeting(s) added, %d skipped, %d error(s). ===",
        success_count,
        skipped_count,
        error_count,
    )


if __name__ == "__main__":
    run()
