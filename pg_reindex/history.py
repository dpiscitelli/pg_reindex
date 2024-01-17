import logging
import time
from pg_reindex.sql_connector import SQLLITEConnector


class History:
    def __init__(self, db, debug=False):
        self.logger = logging.getLogger("global_log")
        self.debug = debug
        self.db = db

    def set_history(self, index, status, message):
        now = int(time.time())
        with SQLLITEConnector(self.db) as history:
            cur = history.connection_db.cursor()
            query = f"""INSERT INTO indexation_history(index_name, indexed_at, status, status_message) VALUES ('{index}', '{now}', '{status}', '{message}')"""
            cur.execute(query)
