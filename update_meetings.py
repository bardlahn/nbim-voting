"""

** update_meetings.py **

Fetches all meeting records from the NBIM Voting Records API via the NBIMVRClient and inserts/updates them into the `meetings` table in the `nbim_data` MySQL database.

Operations are logged to update_meetings.log.

Reads database credentials from client/secrets.txt:
    DB_USER=<username>
    DB_SECRET=<password>

"""

import argparse
import logging
import sys
from datetime import datetime

import mysql.connector
from mysql.connector import Error as MySQLError

from client.nbimvr_client import NBIMVRClient


# THIS IS STILL UNDER CONSTRUCTION! 
# CHECK BACK LATER