import logging
import time
import os
from datetime import datetime
from pg_reindex.sql_connector import SQLLITEConnector


class HistoryException(Exception):
    pass


class History:
    def __init__(self, db, debug=False):
        self.logger = logging.getLogger("global_log")
        self.debug = debug
        self.db = db

    def set_or_update_history(self, db, schema, table, index, status, message):
        now = int(time.time())
        now_timestamp = datetime.now()
        with SQLLITEConnector(self.db) as history:
            cur = history.connection_db.cursor()
            query = f"""SELECT COUNT(*) FROM indexation_history WHERE index_name = '{index}'"""
            cur.execute(query)
            result = cur.fetchone()
            if result[0] > 0:
                query = f"""UPDATE indexation_history 
                            SET 
                                database_name = '{db}', 
                                indexed_at = '{now}', 
                                status = '{status}', 
                                status_message = '{message}', 
                                timestamp_message = '{now_timestamp}' 
                            WHERE index_name = '{index}'"""
            else:
                query = f"""INSERT INTO indexation_history(database_name, index_name, indexed_at, status, status_message, timestamp_message) 
                            VALUES ('{db}', '{index}', '{now}', '{status}', '{message}', '{now_timestamp}')"""
            cur.execute(query)

    def get_reindex_status(self, db, index):
        with SQLLITEConnector(self.db) as history:
            cur = history.connection_db.cursor()
            query = f"""SELECT status FROM indexation_history WHERE database_name = '{db}' AND index_name = '{index}'"""
            cur.execute(query)
            result = cur.fetchone()
            if result:
                return result[0]
            else:
                return None

    def drop_history(self):
        os.remove(self.db)
