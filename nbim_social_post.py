"""

** nbim_social_post.py **

Checks the database for meetings, identifies any votes where NBIM's vote_instruction deviates
from management_rec, and prepares a short social media post for each such meeting.

Operations are logged to nbim_social_post.log.

Optional arguments:
    --date DATE                 Override today's date (format: YYYY-MM-DD).
    --timing NEAR|EXACT|UPDATED How to apply the date when selecting meetings (default: NEAR).
                                  NEAR    — meetings whose 'date' falls within the 14 days up to
                                            and including the target date.
                                  EXACT   — meetings whose 'date' equals the target date exactly.
                                  UPDATED — meetings whose 'updated' field equals the target date
                                            exactly (i.e. rows added on that date).
    --include NEW|ALL           Which meetings to process (default: NEW).
                                  NEW — skip meetings where 'posted' is already set.
                                  ALL — process all matched meetings regardless of 'posted'.
    --dry-run               Print posts to terminal only; do not invoke posting function.
    --log OFF|STRICT|FULL   File logging level (default: STRICT).

"""

import argparse
import sys
from datetime import date, timedelta

from mysql.connector import Error as MySQLError

from atproto import Client, client_utils

from nbim_functions_db import connect_db, _load_secrets
from nbim_functions_shared import setup_logging, configure_file_logging, log_important

log = setup_logging("nbim_social_post", "nbim_social_post.log")


# Defining database queries

_GET_MEETINGS_NEAR_SQL = """
SELECT id, type, date, company_name, posted
FROM meetings
WHERE date BETWEEN %(from_date)s AND %(to_date)s;
"""

_GET_MEETINGS_EXACT_SQL = """
SELECT id, type, date, company_name, posted
FROM meetings
WHERE date = %(target_date)s;
"""

_GET_MEETINGS_UPDATED_SQL = """
SELECT id, type, date, company_name, posted
FROM meetings
WHERE DATE(updated) = %(target_date)s;
"""

_GET_DEVIATING_VOTES_SQL = """
SELECT proposal_text, proponent, management_rec, vote_instruction
FROM votes
WHERE meeting_id = %(meeting_id)s
AND vote_instruction != management_rec;
"""

_SET_MEETING_POSTED_SQL = """
UPDATE meetings
SET posted = %(posted_date)s
WHERE id = %(meeting_id)s;
"""


# Database functions

def get_meetings(conn, target_date: str, timing: str) -> list[dict]:
    cur = conn.cursor(dictionary=True)
    if timing == "NEAR":
        from_date = (date.fromisoformat(target_date) - timedelta(days=14)).strftime("%Y-%m-%d")
        to_date = (date.fromisoformat(target_date) + timedelta(days=14)).strftime("%Y-%m-%d")
        cur.execute(_GET_MEETINGS_NEAR_SQL, {"from_date": from_date, "to_date": to_date})
    elif timing == "EXACT":
        cur.execute(_GET_MEETINGS_EXACT_SQL, {"target_date": target_date})
    else:  # UPDATED
        cur.execute(_GET_MEETINGS_UPDATED_SQL, {"target_date": target_date})
    rows = cur.fetchall()
    cur.close()
    return rows


def get_deviating_votes(conn, meeting_id: int) -> list[dict]:
    cur = conn.cursor(dictionary=True)
    cur.execute(_GET_DEVIATING_VOTES_SQL, {"meeting_id": meeting_id})
    rows = cur.fetchall()
    cur.close()
    return rows


def set_meeting_posted(conn, meeting_id: int, posted_date: str) -> None:
    cur = conn.cursor()
    cur.execute(_SET_MEETING_POSTED_SQL, {"posted_date": posted_date, "meeting_id": meeting_id})
    conn.commit()
    cur.close()


# Post formatting

def format_post(meeting: dict, votes: list[dict]) -> str:
    
    meetname = meeting["type"]
    if not meetname.lower().endswith("meeting"):
        meetname += " meeting"
    intro = "%s %s (%s) - NBIM voted against management:\n" % (meeting["company_name"], meetname, meeting["date"])
    lines = []

    for i, vote in enumerate(votes, start=1):
        lines.append("• %s %s proposal: %s" % (
            vote["vote_instruction"],
            vote["proponent"],
            truncate_string(vote["proposal_text"]),
        ))
    
    d = intro + combine_lines(intro, lines)
    return d


def truncate_string(s: str) -> str:
    if len(s) <= 26:
        return s
    truncate_pos = s.find(' ', 20) 
    if truncate_pos == -1:
        return s[:20] + '…'
    return s[:truncate_pos] + '…'


def combine_lines(s1, arr: list[str]) -> str:
    base_length = len(s1)
    total_length = base_length + sum(len(s) for s in arr)

    if total_length <= 260:
        return '\n'.join(arr) + "\nSee "

    combined = ''
    for item in arr:
        addition = item if not combined else '\n' + item
        if base_length + len(combined) + len(addition) > 245:
            combined += "\nFor more votes, see "
            break
        combined += addition

    return combined


# Bluesky posting

def post_bluesky(post: str, meeting: int) -> None:

    secrets = _load_secrets()
    client = Client()
    client.login(secrets["BSKY_HANDLE"], secrets["BSKY_PASS"]) 
    text_builder = client_utils.TextBuilder()

    baseurl = "https://www.nbim.no/en/responsible-investment/voting/our-voting-records/meeting?m="

    text_builder.text(post)
    text_builder.link("full meeting details", baseurl + str(meeting))
    text_builder.text(".")

    post_text = text_builder.build_text()
    post_facets = text_builder.build_facets()
    client.send_post(text=post_text, facets=post_facets)

    pass


# Argument parsing

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Prepare social media posts for NBIM voting deviations.")
    p.add_argument("--date", default=None, metavar="DATE",
                   help="Override today's date for checking meetings (format: YYYY-MM-DD).")
    p.add_argument("--timing", choices=["NEAR", "EXACT", "UPDATED"], default="NEAR",
                   help="How to apply the date when selecting meetings: NEAR (14-day window, default), "
                        "EXACT (exact date match on 'date' field), or UPDATED (exact match on 'updated' field).")
    p.add_argument("--include", choices=["NEW", "ALL"], default="NEW",
                   help="Which meetings to process: NEW (skip already-posted, default) or ALL (ignore 'posted' field).")
    p.add_argument("--dry-run", action="store_true",
                   help="Print posts to terminal only; do not invoke posting function.")
    p.add_argument("--log", choices=["OFF", "STRICT", "FULL"], default="STRICT",
                   help="File logging level: OFF, STRICT (errors only, default), or FULL.")
    p.add_argument("--limit", type=int, default=None, metavar="N",
               help="Maximum number of posts to publish to Bluesky in one run. Oldest meetings are posted first.")
    args = p.parse_args()
    if args.date is not None:
        try:
            date.fromisoformat(args.date)
        except ValueError:
            p.error("--date must be in YYYY-MM-DD format.")
    return args


# Main logic

def run() -> None:
    args = parse_args()
    configure_file_logging(log, args.log)

    today = args.date if args.date else date.today().strftime("%Y-%m-%d")
    execution_date = date.today().strftime("%Y-%m-%d")

    start_msg = "=== nbim_social_post.py started at %s (target date: %s, --timing %s, --include %s)" % (
        date.today().strftime("%Y-%m-%d %H:%M:%S"), today, args.timing, args.include)
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

    # Fetch meetings according to --timing
    try:
        meetings = get_meetings(conn, today, args.timing)
        log.info("Found %d meeting(s) matching target date %s (--timing %s).", len(meetings), today, args.timing)
    except MySQLError as exc:
        log.error("Failed to fetch meetings: %s — aborting.", exc)
        conn.close()
        sys.exit(1)

    if not meetings:
        log_important(log, "=== No meetings found for target date %s (--timing %s). ===" % (today, args.timing))
        conn.close()
        return

    # Filter out already-posted meetings if --include NEW
    if args.include == "NEW":
        unposted = [m for m in meetings if not m["posted"]]
        skipped = len(meetings) - len(unposted)
        if skipped:
            log.info("Skipping %d already-posted meeting(s) (--include NEW).", skipped)
        meetings = unposted

    if not meetings:
        log_important(log, "=== All matched meetings already posted. Nothing to process. ===")
        conn.close()
        return
    
    # Sorting meetings to process the oldest first
    meetings.sort(key=lambda m: m["date"])

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

        # If no deviating votes found, moving on without posting
        if not deviating_votes:
            log.info("No deviating votes for meeting id=%s (%s).", meeting["id"], meeting["company_name"])
            no_deviation_count += 1
            if not args.dry_run:
                try:
                    set_meeting_posted(conn, meeting["id"], execution_date)
                    log.info("Set posted=%s for meeting id=%s.", execution_date, meeting["id"])
                except MySQLError as exc:
                    log.error("ERROR setting posted for meeting id=%s: %s", meeting["id"], exc)
                    error_count += 1
            continue

        # If deviating votes are found, checking if post limit is reached
        if args.limit is not None and post_count >= args.limit:
            log.info("Post limit of %d reached; skipping meeting id=%s.", args.limit, meeting["id"])
            continue

        # If post limit is not reached, proceeding to preparing post
        post = format_post(meeting, deviating_votes)
        log.info("Post prepared for meeting id=%s (%s) with %d deviating vote(s).",
                 meeting["id"], meeting["company_name"], len(deviating_votes))

        # If not in dry-run mode, posting the prepared post
        if not args.dry_run:
            try:
                set_meeting_posted(conn, meeting["id"], execution_date)
                log.info("Set posted=%s for meeting id=%s.", execution_date, meeting["id"])
            except MySQLError as exc:
                log.error("ERROR setting posted for meeting id=%s: %s", meeting["id"], exc)
                error_count += 1
                continue
            post_bluesky(post, meeting["id"])
            log.info("Posted to Bluesky for meeting id=%s.", meeting["id"])
        else:
            print("\n" + "─" * 60)
            print(post)
            print("─" * 60)

        post_count += 1


    # Finishing up
    conn.close()
    log_important(log, "=== Finished. %d post(s) prepared, %d meeting(s) with no deviations, %d error(s). ===" % (
        post_count, no_deviation_count, error_count))


if __name__ == "__main__":
    run()
