import logging
import os
import sqlite3


class History:
    def __init__(self, db, debug=False, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.debug = debug
        self.db = db
        self.conn = sqlite3.connect(db)

    def reset_db(self):
        os.remove(self.db)


