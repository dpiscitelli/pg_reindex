import abc
import logging
from psycopg import Connection
import sqlite3
import os


class SQLConnector:
    """
    classe dealing with connection and deconnection
    """

    def __init__(
        self, uri, autocommit=True, application_name="pg_reindex", logger=None
    ):
        self.autocommit = autocommit
        self.application_name = application_name
        self.connection_db = Connection.connect(uri)
        self.logger = logger or logging.getLogger("global_log")

    def __enter__(self):
        """
        method to support with syntax
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        to support with syntax
        disconnect program from database
        """

        if (exc_type is None) and (exc_val is None) and (exc_tb is None):
            if self.autocommit and self.connection_db:
                self.logger.debug("commit automatique de la transaction")
                self.connection_db.commit()
            else:
                self.logger.debug("fin de bloc sans commit de la transaction")

        # do not release connection without endend transaction
        self.connection_db.rollback()
        self.disconnect()

    def disconnect(self):
        """
        disconnect from database
        """
        self.logger.debug("déconnexion de la base de données")
        if (self.connection_db is not None) and (self.connection_db.closed == 0):
            self.connection_db.close()

    def commit(self):
        """
        commit current transaction
        """

        self.logger.debug("commit de la transaction")
        self.connection_db.commit()

    def rollback(self):
        """
        rollback current transaction
        """

        self.logger.debug("rollback de la transaction")
        self.connection_db.rollback()


class SQLLITEConnector:
    """
    classe dealing with connection and deconnection
    """

    def __init__(self, db, logger=None):
        self.db = db
        self.logger = logger or logging.getLogger("global_log")

    def init_db(self):
        conn = sqlite3.connect(self.db)
        cur = conn.cursor()
        query = """CREATE TABLE indexation_history (
                        database_name TEXT, 
                        schema_name TEXT,
                        table_name TEXT,
                        index_name TEXT, 
                        indexed_at INTEGER, 
                        status INTEGER, 
                        status_message TEXT, 
                        timestamp_message TEXT, 
                        CONSTRAINT history_pk PRIMARY KEY(database_name, index_name)
                )"""
        cur.execute(query)
        conn.commit()
        query = "CREATE INDEX indexation_history_idx ON indexation_history(index_name)"
        cur.execute(query)
        conn.commit()
        conn.close()
        self.logger.debug(f"Initialize sqllite db =>{self.db}")

    def __enter__(self):
        """
        method to support with syntax
        """
        if not os.path.exists(self.db):
            self.init_db()
        self.connection_db = sqlite3.connect(self.db)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        to support with syntax
        disconnect program from database
        """

        if (exc_type is None) and (exc_val is None) and (exc_tb is None):
            if self.connection_db:
                self.logger.debug("commit automatique de la transaction")
                self.connection_db.commit()
            else:
                self.logger.debug("fin de bloc sans commit de la transaction")

        # do not release connection without endend transaction
        self.connection_db.rollback()
        self.connection_db.close()
