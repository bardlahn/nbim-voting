"""

** update_companies.py **

Fetches all company records from the NBIM Voting Records API via the NBIMVRClient and inserts/updates them into the `companies` table in the `nbim_data` MySQL database.

Operations are logged to update_companies.log.

Reads database credentials from client/secrets.txt:
    DB_USER=<username>
    DB_SECRET=<password>

Optional arguments:
    --letter A      Only process companies whose name starts with this letter (case-insensitive).
    --add           Only add companies that do not already exist in the database.
    --limit N       Stop after fetching N companies.

"""

import argparse
import logging
import sys
from datetime import datetime

import mysql.connector
from mysql.connector import Error as MySQLError

from client.nbimvr_client import NBIMVRClient


# Setting up logging

log = logging.getLogger("update_companies")
log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

file_handler = logging.FileHandler("update_companies.log", encoding="utf-8")
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

DDL = """
CREATE TABLE IF NOT EXISTS companies (
    id          INT             PRIMARY KEY,
    name        VARCHAR(512)    NOT NULL,
    isin        VARCHAR(32),
    ticker      VARCHAR(64),
    country     VARCHAR(128),
    meetings    TEXT,
    updated     DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

UPSERT_SQL = """
INSERT INTO companies (id, name, isin, ticker, country, meetings, updated)
VALUES (%(id)s, %(name)s, %(isin)s, %(ticker)s, %(country)s, %(meetings)s, %(updated)s)
ON DUPLICATE KEY UPDATE
    name     = VALUES(name),
    isin     = VALUES(isin),
    ticker   = VALUES(ticker),
    country  = VALUES(country),
    meetings = VALUES(meetings),
    updated  = VALUES(updated);
"""

EXISTS_SQL = "SELECT 1 FROM companies WHERE name = %(name)s LIMIT 1;"


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


def ensure_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(DDL)
    conn.commit()
    cur.close()


def company_exists(conn, name: str) -> bool:
    cur = conn.cursor()
    cur.execute(EXISTS_SQL, {"name": name})
    result = cur.fetchone()
    cur.close()
    return result is not None


def upsert_company(conn, row: dict) -> None:
    cur = conn.cursor()
    cur.execute(UPSERT_SQL, row)
    conn.commit()
    cur.close()


# Argument parsing

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Populate/update the companies table from the NBIM API.")
    p.add_argument(
        "--letter",
        default=None,
        metavar="LETTER",
        help="Only process companies whose name starts with this letter (case-insensitive).",
    )
    p.add_argument(
        "--add",
        action="store_true",
        help="Only add companies not already present in the database; skip existing ones.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Stop after fetching N companies.",
    )
    args = p.parse_args()
    if args.letter is not None:
        if len(args.letter) != 1 or not args.letter.isalpha():
            p.error("--letter must be a single letter, e.g.: python update_companies.py --letter M")
    if args.limit is not None and args.limit < 1:
        p.error("--limit must be a positive integer.")
    return args


# Main functionality

def run() -> None:
    args = parse_args()
    letter = args.letter.upper() if args.letter else None

    start_msg = "=== update_companies.py started at %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if letter:
        start_msg += " --letter %s" % letter
    if args.add:
        start_msg += " --add"
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
        ensure_table(conn)
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

    # Fetch all company names
    try:
        company_names = client.get_company_names()
        log.info("Retrieved %d company name(s) from API.", len(company_names))
    except Exception as exc:
        log.error("Failed to fetch company names from API: %s — aborting.", exc)
        conn.close()
        sys.exit(1)

    # Filter by letter if provided
    if letter:
        company_names = [n for n in company_names if n.upper().startswith(letter)]
        log.info("Filtered to %d company name(s) starting with '%s'.", len(company_names), letter)

    # Apply limit if provided
    if args.limit:
        company_names = company_names[:args.limit]
        log.info("Limiting to %d company name(s).", len(company_names))

    # Process each company
    success_count = 0
    error_count = 0
    skipped_count = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for name in company_names:
        # Skip existing companies if --add is set
        if args.add:
            try:
                if company_exists(conn, name):
                    log.debug("SKIP '%s' — already exists in database.", name)
                    skipped_count += 1
                    continue
            except MySQLError as exc:
                log.error("ERROR checking existence of '%s' in database: %s", name, exc)
                error_count += 1
                continue

        # Fetch full company record(s) by name
        try:
            companies = client.query_company_with_name(name)
        except Exception as exc:
            log.error("ERROR fetching company '%s' from API: %s", name, exc)
            error_count += 1
            continue

        if not companies:
            log.error("ERROR no records returned for company name '%s'.", name)
            error_count += 1
            continue

        for company in companies:
            # Validate required fields
            warnings = []
            if not company.id:
                warnings.append("id is blank")
            if not company.name:
                warnings.append("name is blank")
            if not company.isin:
                warnings.append("isin is blank")
            if not company.ticker:
                warnings.append("ticker is blank")
            if not company.country:
                warnings.append("country is blank")
            if warnings:
                log.warning(
                    "WARNING company '%s' (id=%s) has blank field(s): %s",
                    company.name or name,
                    company.id,
                    ", ".join(warnings),
                )

            if not company.id:
                log.error(
                    "ERROR skipping company '%s' — no id returned by API.",
                    company.name or name,
                )
                error_count += 1
                continue

            # Extract meeting IDs
            meetings_str = None
            if company.meetings:
                meeting_ids = []
                for meeting in company.meetings:
                    if hasattr(meeting, "id") and meeting.id is not None:
                        meeting_ids.append(str(meeting.id))
                    else:
                        log.warning(
                            "WARNING company id=%s has a meeting entry with no id.",
                            company.id,
                        )
                meetings_str = ",".join(meeting_ids) if meeting_ids else None

            row = {
                "id":       company.id,
                "name":     company.name,
                "isin":     company.isin,
                "ticker":   company.ticker,
                "country":  company.country,
                "meetings": meetings_str,
                "updated":  now,
            }

            # Upsert into database
            try:
                upsert_company(conn, row)
                log.info("OK  id=%-8s  %s", company.id, company.name)
                success_count += 1
            except MySQLError as exc:
                log.error(
                    "ERROR could not write company id=%s name='%s' to database: %s",
                    company.id, company.name, exc,
                )
                error_count += 1

    conn.close()
    log.info(
        "=== Finished. %d updated, %d skipped, %d error(s). ===",
        success_count,
        skipped_count,
        error_count,
    )


if __name__ == "__main__":
    run()
