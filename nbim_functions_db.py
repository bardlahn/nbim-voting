"""

** nbim_db_functions.py **

Shared database helper functions for the NBIM data harvesting scripts.

Provides:
    - connect_db()                  Read secrets and return a database connection
    - ensure_table(conn, ddl)       Create a table if it does not exist
    - company_exists(conn, name)
    - upsert_company(conn, row)
    - meeting_exists(conn, meeting_id)
    - insert_meeting(conn, meeting, now)
    - get_all_meeting_ids(conn)

"""

import json

import mysql.connector
from mysql.connector import Error as MySQLError


# ──────────────────────────────────────────────
# Secrets
# ──────────────────────────────────────────────

def _load_secrets(path: str = "client/secrets.txt") -> dict:
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
# Connection
# ──────────────────────────────────────────────

def connect_db(secrets_path: str = "client/secrets.txt"):
    """Read credentials from secrets file and return a MySQL connection."""
    secrets = _load_secrets(secrets_path)
    db_user = secrets["DB_USER"]
    db_password = secrets["DB_SECRET"]
    return mysql.connector.connect(
        host="localhost",
        port=3306,
        database="nbim_data",
        user=db_user,
        password=db_password,
        charset="utf8mb4",
        autocommit=False,
    )


# ──────────────────────────────────────────────
# Table management
# ──────────────────────────────────────────────

def ensure_table(conn, ddl: str) -> None:
    """Execute a CREATE TABLE IF NOT EXISTS statement."""
    cur = conn.cursor()
    cur.execute(ddl)
    conn.commit()
    cur.close()


# ──────────────────────────────────────────────
# Companies
# ──────────────────────────────────────────────

_COMPANY_EXISTS_SQL = "SELECT 1 FROM companies WHERE name = %(name)s LIMIT 1;"

_UPSERT_COMPANY_SQL = """
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


def company_exists(conn, name: str) -> bool:
    """Return True if a company with the given name exists in the companies table."""
    cur = conn.cursor()
    cur.execute(_COMPANY_EXISTS_SQL, {"name": name})
    result = cur.fetchone()
    cur.close()
    return result is not None


def upsert_company(conn, row: dict) -> None:
    """Insert or update a company row in the companies table."""
    cur = conn.cursor()
    cur.execute(_UPSERT_COMPANY_SQL, row)
    conn.commit()
    cur.close()


# ──────────────────────────────────────────────
# Meetings
# ──────────────────────────────────────────────

_MEETING_EXISTS_SQL = "SELECT 1 FROM meetings WHERE id = %(id)s LIMIT 1;"

_INSERT_MEETING_SQL = """
INSERT INTO meetings (id, type, date, company_id, company_name, company_ticker, isin, updated)
VALUES (%(id)s, %(type)s, %(date)s, %(company_id)s, %(company_name)s, %(company_ticker)s, %(isin)s, %(updated)s);
"""

_INSERT_VOTE_SQL = """
INSERT INTO votes
    (item_on_agenda_id, meeting_id, proposal_number, proposal_sequence,
     proposal_text, proponent, management_rec, vote_instruction, voter_rationale)
VALUES
    (%(item_on_agenda_id)s, %(meeting_id)s, %(proposal_number)s, %(proposal_sequence)s,
     %(proposal_text)s, %(proponent)s, %(management_rec)s, %(vote_instruction)s, %(voter_rationale)s);
"""


def meeting_exists(conn, meeting_id: int) -> bool:
    """Return True if a meeting with the given ID exists in the meetings table."""
    cur = conn.cursor()
    cur.execute(_MEETING_EXISTS_SQL, {"id": meeting_id})
    result = cur.fetchone()
    cur.close()
    return result is not None


_GET_ALL_MEETINGS_SQL = "SELECT meetings FROM companies WHERE meetings IS NOT NULL AND meetings != '';"


def get_all_meeting_ids(conn) -> list[int]:
    """Return a deduplicated list of all meeting IDs found in the companies table."""
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


def insert_meeting(conn, meeting, now: str) -> None:
    """Insert a meeting and all its votes into the database as a single transaction."""
    cur = conn.cursor()

    # Strip time component from date if present
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
    cur.execute(_INSERT_MEETING_SQL, meeting_row)

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
        cur.execute(_INSERT_VOTE_SQL, vote_row)

    conn.commit()
    cur.close()
