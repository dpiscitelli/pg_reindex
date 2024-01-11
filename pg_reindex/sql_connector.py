import abc
import logging
from psycopg import conninfo, Connection


class SQLConnector(metaclass=abc.ABCMeta):
    """
    classe dealing with connection and deconnection
    """

    def __init__(
        self, uri, autocommit=True, application_name="pg_reindex", logger=None
    ):
        self.autocommit = autocommit
        self.application_name = application_name
        self.connection_db = Connection.connect(uri)
        self.logger = logger or logging.getLogger(__name__)

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
