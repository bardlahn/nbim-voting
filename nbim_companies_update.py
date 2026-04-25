"""

** nbim_companies_update.py **

Fetches all company records from the NBIM Voting Records API via the NBIMVRClient and inserts/updates them into the `companies` table in the `nbim_data` MySQL database.

Operations are logged to nbim_companies_update.log.

Optional arguments:
    --letter A          Only process companies whose name starts with this letter (case-insensitive).
    --add               Only add companies not already present in the database; skip existing ones.
    --limit N           Stop after N API requests. Skipped companies (--add) do not count towards limit.
    --staged NAME       Staged run using batch NAME. On first run, writes company names to NAME.tmp
                        and processes from that list. On subsequent runs, continues from the remaining
                        unprocessed names in NAME.tmp. Deletes NAME.tmp when all names are processed.
    --log OFF|STRICT|FULL
                        File logging level (default: STRICT).
                        OFF    = no logging to file.
                        STRICT = only start, end, and error messages.
                        FULL   = all messages.

"""

import argparse
import os
import sys
from datetime import datetime

from mysql.connector import Error as MySQLError

from client.nbimvr_client import NBIMVRClient
from nbim_functions_db import connect_db, ensure_table, company_exists, upsert_company
from nbim_functions_shared import setup_logging, configure_file_logging, log_important


# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────

log = setup_logging("nbim_companies_update", "nbim_companies_update.log")


# ──────────────────────────────────────────────
# DDL
# ──────────────────────────────────────────────

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


# ──────────────────────────────────────────────
# Staged run helpers
# ──────────────────────────────────────────────

def staged_file_path(name: str) -> str:
    return "%s.tmp" % name


def read_staged_file(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [line.rstrip("\n") for line in f if line.strip()]


def write_staged_file(path: str, names: list[str]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for name in names:
            f.write(name + "\n")


# ──────────────────────────────────────────────
# Argument parsing
# ──────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Populate/update the companies table from the NBIM API.")
    p.add_argument("--letter", default=None, metavar="LETTER",
                   help="Only process companies whose name starts with this letter (case-insensitive).")
    p.add_argument("--add", action="store_true",
                   help="Only add companies not already present in the database; skip existing ones.")
    p.add_argument("--limit", type=int, default=None, metavar="N",
                   help="Stop after N API requests (skipped companies do not count).")
    p.add_argument("--staged", default=None, metavar="NAME",
                   help="Staged run: persist progress to NAME.tmp and continue across runs.")
    p.add_argument("--log", choices=["OFF", "STRICT", "FULL"], default="STRICT",
                   help="File logging level: OFF, STRICT (errors only, default), or FULL.")
    args = p.parse_args()
    if args.letter is not None:
        if len(args.letter) != 1 or not args.letter.isalpha():
            p.error("--letter must be a single letter, e.g.: python nbim_companies_update.py --letter M")
    if args.limit is not None and args.limit < 1:
        p.error("--limit must be a positive integer.")
    if args.staged is not None and not args.staged.isalnum():
        p.error("--staged NAME must be alphanumeric.")
    return args


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def run() -> None:
    args = parse_args()
    configure_file_logging(log, args.log)
    letter = args.letter.upper() if args.letter else None

    start_msg = "=== nbim_companies_update.py started at %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if letter:
        start_msg += " --letter %s" % letter
    if args.add:
        start_msg += " --add"
    if args.limit:
        start_msg += " --limit %d" % args.limit
    if args.staged:
        start_msg += " --staged %s" % args.staged
    if args.log != "STRICT":
        start_msg += " --log %s" % args.log
    start_msg += " ==="
    log_important(log, start_msg)

    # Connect to database
    try:
        conn = connect_db()
        ensure_table(conn, DDL)
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

    # Determine company names list — from staged file or fresh API call
    staged_path = staged_file_path(args.staged) if args.staged else None

    if staged_path and os.path.exists(staged_path):
        company_names = read_staged_file(staged_path)
        log_important(log, "Staged run '%s': resuming with %d remaining name(s) from %s." % (
            args.staged, len(company_names), staged_path))
    else:
        try:
            company_names = client.get_company_names()
            log.info("Retrieved %d company name(s) from API.", len(company_names))
        except Exception as exc:
            log.error("Failed to fetch company names from API: %s — aborting.", exc)
            conn.close()
            sys.exit(1)

        if letter:
            company_names = [n for n in company_names if n.upper().startswith(letter)]
            log.info("Filtered to %d company name(s) starting with '%s'.", len(company_names), letter)

        if staged_path:
            write_staged_file(staged_path, company_names)
            log_important(log, "Staged run '%s': wrote %d name(s) to %s." % (
                args.staged, len(company_names), staged_path))

    # Process each company
    success_count = 0
    error_count = 0
    skipped_count = 0
    fetch_count = 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    remaining_names = list(company_names)

    for name in company_names:
        # Skip existing companies if --add is set (does not count towards limit)
        if args.add:
            try:
                if company_exists(conn, name):
                    log.debug("SKIP '%s' — already exists in database.", name)
                    skipped_count += 1
                    remaining_names.remove(name)
                    if staged_path:
                        write_staged_file(staged_path, remaining_names)
                    continue
            except MySQLError as exc:
                log.error("ERROR checking existence of '%s' in database: %s", name, exc)
                error_count += 1
                remaining_names.remove(name)
                if staged_path:
                    write_staged_file(staged_path, remaining_names)
                continue

        # Stop if API fetch limit has been reached
        if args.limit and fetch_count >= args.limit:
            log.info("API fetch limit of %d reached — stopping.", args.limit)
            break

        # Fetch full company record(s) by name
        try:
            companies = client.query_company_with_name(name)
            fetch_count += 1
        except Exception as exc:
            log.error("ERROR fetching company '%s' from API: %s", name, exc)
            error_count += 1
            remaining_names.remove(name)
            if staged_path:
                write_staged_file(staged_path, remaining_names)
            continue

        if not companies:
            log.error("ERROR no records returned for company name '%s'.", name)
            error_count += 1
            remaining_names.remove(name)
            if staged_path:
                write_staged_file(staged_path, remaining_names)
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
                log.warning("WARNING company '%s' (id=%s) has blank field(s): %s",
                            company.name or name, company.id, ", ".join(warnings))

            if not company.id:
                log.error("ERROR skipping company '%s' — no id returned by API.", company.name or name)
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
                        log.warning("WARNING company id=%s has a meeting entry with no id.", company.id)
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

            try:
                upsert_company(conn, row)
                log.info("OK  id=%-8s  %s", company.id, company.name)
                success_count += 1
            except MySQLError as exc:
                log.error("ERROR could not write company id=%s name='%s' to database: %s",
                          company.id, company.name, exc)
                error_count += 1

        # Mark name as completed in staged file
        remaining_names.remove(name)
        if staged_path:
            write_staged_file(staged_path, remaining_names)

    # Clean up staged file if all names have been processed
    if staged_path and os.path.exists(staged_path) and not remaining_names:
        os.remove(staged_path)
        log_important(log, "Staged run '%s': all names processed, %s deleted." % (args.staged, staged_path))

    conn.close()
    log_important(log, "=== Finished. %d updated, %d skipped, %d error(s). ===" % (
        success_count, skipped_count, error_count))


if __name__ == "__main__":
    run()
