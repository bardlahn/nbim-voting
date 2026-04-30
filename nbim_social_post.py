"""

** nbim_social_post.py **

Checks the database for meetings held on today's date, identifies any votes
where NBIM's vote_instruction deviates from management_rec, and prepares a
short social media post for each such meeting.

Posts are currently printed to the terminal. Posting mechanics will be added later.

Operations are logged to nbim_social_post.log.

Optional arguments:
    --date DATE             Override today's date for checking meetings (format: YYYY-MM-DD).
    --dry-run               Print posts to terminal only; do not invoke posting function.
    --log OFF|STRICT|FULL   File logging level (default: STRICT).

"""

import argparse
import sys
from datetime import date

from mysql.connector import Error as MySQLError

from atproto import Client, client_utils

from nbim_functions_db import connect_db, _load_secrets
from nbim_functions_shared import setup_logging, configure_file_logging, log_important


# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────

log = setup_logging("nbim_social_post", "nbim_social_post.log")


# ──────────────────────────────────────────────
# Database queries
# ──────────────────────────────────────────────

_GET_TODAYS_MEETINGS_SQL = """
SELECT id, type, date, company_name
FROM meetings
WHERE date = %(today)s;
"""

_GET_DEVIATING_VOTES_SQL = """
SELECT proposal_text, proponent, management_rec, vote_instruction
FROM votes
WHERE meeting_id = %(meeting_id)s
AND vote_instruction != management_rec;
"""


def get_meetings(conn, today: str) -> list[dict]:
    cur = conn.cursor(dictionary=True)
    cur.execute(_GET_TODAYS_MEETINGS_SQL, {"today": today})
    rows = cur.fetchall()
    cur.close()
    return rows


def get_deviating_votes(conn, meeting_id: int) -> list[dict]:
    cur = conn.cursor(dictionary=True)
    cur.execute(_GET_DEVIATING_VOTES_SQL, {"meeting_id": meeting_id})
    rows = cur.fetchall()
    cur.close()
    return rows


# ──────────────────────────────────────────────
# Post formatting
# ──────────────────────────────────────────────

def format_post(meeting: dict, votes: list[dict]) -> str:
    header = "%s: NBIM voted against management in %s meeting (%s)." % (
        meeting["company_name"],
        meeting["type"],
        meeting["date"],
    )
    lines = [header]
    for i, vote in enumerate(votes, start=1):
        lines.append("- Voted '%s' on %s proposal: %s" % (
            vote["vote_instruction"],
            vote["proponent"],
            vote["proposal_text"],
        ))
    return "\n".join(lines)


# ──────────────────────────────────────────────
# Social media posting
# ──────────────────────────────────────────────

def post_bluesky(text: str, meeting: str) -> None:

    # Posting a message to Bluesky

    secrets = _load_secrets()
    client = Client()
    client.login(secrets["BSKY_HANDLE"], secrets["BSKY_PASS"]) 
    text_builder = client_utils.TextBuilder()

    baseurl = "https://www.nbim.no/en/responsible-investment/voting/our-voting-records/meeting?m="

    text_builder.text(text)
    text_builder.text("(See ")
    text_builder.link("full meeting details", baseurl + meeting)
    text_builder.text(")")

    post_text = text_builder.build_text()
    post_facets = text_builder.build_facets()
    client.send_post(text=post_text, facets=post_facets)

    pass


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Prepare social media posts for NBIM voting deviations.")
    p.add_argument("--date", default=None, metavar="DATE",
                   help="Override today's date for checking meetings (format: YYYY-MM-DD).")
    p.add_argument("--dry-run", action="store_true",
                   help="Print posts to terminal only; do not invoke posting function.")
    p.add_argument("--log", choices=["OFF", "STRICT", "FULL"], default="STRICT",
                   help="File logging level: OFF, STRICT (errors only, default), or FULL.")
    args = p.parse_args()
    if args.date is not None:
        try:
            date.fromisoformat(args.date)
        except ValueError:
            p.error("--date must be in YYYY-MM-DD format.")
    return args


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def run() -> None:
    args = parse_args()
    configure_file_logging(log, args.log)

    today = args.date if args.date else date.today().strftime("%Y-%m-%d")

    start_msg = "=== nbim_social_post.py started at %s (checking date: %s)" % (
        date.today().strftime("%Y-%m-%d %H:%M:%S"), today)
    if args.dry_run:
        start_msg += " --dry-run"
    if args.log != "STRICT":
        start_msg += " --log %s" % args.log
    start_msg += " ==="
    log_important(log, start_msg)

    # Connect to database
    try:
        conn = connect_db()
        log.info("Connected to database `nbim_data`.")
    except (MySQLError, KeyError, FileNotFoundError) as exc:
        log.error("Could not connect to database: %s — aborting.", exc)
        sys.exit(1)

    # Fetch today's meetings
    try:
        meetings = get_meetings(conn, today)
        log.info("Found %d meeting(s) with date %s.", len(meetings), today)
    except MySQLError as exc:
        log.error("Failed to fetch meetings: %s — aborting.", exc)
        conn.close()
        sys.exit(1)

    if not meetings:
        log_important(log, "=== No meetings found for date %s. ===" % today)
        conn.close()
        return

    # Process each meeting
    post_count = 0
    no_deviation_count = 0
    error_count = 0

    for meeting in meetings:
        try:
            deviating_votes = get_deviating_votes(conn, meeting["id"])
        except MySQLError as exc:
            log.error("ERROR fetching votes for meeting id=%s: %s", meeting["id"], exc)
            error_count += 1
            continue

        if not deviating_votes:
            log.info("No deviating votes for meeting id=%s (%s).", meeting["id"], meeting["company_name"])
            no_deviation_count += 1
            continue

        post = format_post(meeting, deviating_votes)
        log.info("Post prepared for meeting id=%s (%s) with %d deviating vote(s).",
                 meeting["id"], meeting["company_name"], len(deviating_votes))

        if args.dry_run:
            print("\n" + "─" * 60)
            print(post)
            print("─" * 60)
        else:
            post_bluesky(post, meeting["id]"])
            log.info("Posted to Bluesky for meeting id=%s.", meeting["id"])

        post_count += 1

    conn.close()
    log_important(log, "=== Finished. %d post(s) prepared, %d meeting(s) with no deviations, %d error(s). ===" % (
        post_count, no_deviation_count, error_count))


if __name__ == "__main__":
    run()
