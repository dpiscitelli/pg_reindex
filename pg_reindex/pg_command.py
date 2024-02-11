import logging
import psycopg
from psycopg import sql
from pg_reindex.sql_connector import SQLConnector
import time
from typing import List, Tuple


class PGCommand:
    """
    A class that represents a PostgreSQL command.

    Attributes:
        uri (str): The URI of the PostgreSQL database.
        lock_timeout (int): The lock timeout value in seconds.
        statement_timeout (int): The statement timeout value in seconds.
        debug (bool): A flag indicating whether to enable debug mode.
    """

    def __init__(
        self,
        uri: str,
        lock_timeout: int = 120,
        statement_timeout: int = 18000,
        debug: bool = False,
    ) -> None:
        """
        Initializes a new instance of the PGCommand class.

        Args:
            uri (str): The URI of the PostgreSQL database.
            lock_timeout (int, optional): The lock timeout value in seconds. Defaults to 120.
            statement_timeout (int, optional): The statement timeout value in seconds. Defaults to 18000.
            debug (bool, optional): A flag indicating whether to enable debug mode. Defaults to False.
        """
        self.uri = uri
        self.logger = logging.getLogger("global_log")
        self.debug = debug
        self.lock_timeout = f"SET LOCK_TIMEOUT = '{lock_timeout}s'"
        self.statement_timeout = f"SET STATEMENT_TIMEOUT = '{statement_timeout}s'"

    def get_indexes(
        self, schema_name: str, table_name: str, index_pattern: str = "%"
    ) -> List[Tuple[str, str, str, str, str, str]]:
        """
        Retrieves the indexes for a given schema and table.

        Args:
            schema_name (str): The name of the schema.
            table_name (str): The name of the table.
            index_pattern (str, optional): The pattern to match index names. Defaults to "%".

        Returns:
            list: A list of index information.
        """
        query = sql.SQL(
            """
            SELECT
                n.nspname AS schemaname,
                c.relname AS tablename,
                i.relname AS indexname,
                CASE
                    WHEN indisprimary THEN 'p'
                    WHEN indisunique THEN 'u'
                    ELSE 'i'
                END as index_type,
                a.amname AS index_method,
                pg_get_indexdef(x.indexrelid)
            FROM pg_index x
            JOIN pg_class c ON c.oid = x.indrelid
            JOIN pg_class i ON i.oid = x.indexrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_am a ON a.oid = i.relam
            WHERE (c.relkind = ANY (ARRAY['r'::"char", 'm'::"char", 'p'::"char"]))
                AND (i.relkind = ANY (ARRAY['i'::"char", 'I'::"char"]))
                AND n.nspname = %s
                AND c.relname = %s
                AND i.relname SIMILAR TO %s
            """
        )
        with SQLConnector(self.uri) as session:
            c = session.connection_db.cursor()
            c.execute(query, [schema_name, table_name, index_pattern])
            index_list = c.fetchall()
            if self.debug is True:
                self.logger.debug(f"PGCommand.get_indexes:: Index list: {index_list}")
            return index_list

    def get_tables_from_schemas(self, schemas: List[str]) -> List[str]:
        """
        Retrieves the tables from the specified schemas.

        Args:
            schemas (list): A list of schema names.

        Returns:
            list: A list of fully qualified table names.
        """
        query = sql.SQL(
            """
            SELECT
                n.nspname||'.'||c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE (c.relkind = ANY (ARRAY['r'::"char", 'm'::"char", 'p'::"char"]))
            AND n.nspname = ANY(%s)
            """
        )
        try:
            with SQLConnector(self.uri) as session:
                c = session.connection_db.cursor()
                c.execute(
                    query,
                    [
                        schemas,
                    ],
                )
                return [x[0] for x in c.fetchall()]
        except psycopg.Error as e:
            self.logger.error(f"Error: {e}")
            self.logger.error(f"Query: {e}")
            return False, e

    def rebuild_index(self, index_name, concurrently=False):
        """
        Rebuilds the specified index.

        Args:
            index_name (str): The name of the index.
            concurrently (bool, optional): Whether to rebuild the index concurrently. Defaults to False.

        Returns:
            tuple: A tuple indicating the success status and the result message.
        """
        if concurrently is True:
            option_concurrently = "CONCURRENTLY"
        else:
            option_concurrently = ""
        query = f"REINDEX INDEX {option_concurrently} {index_name}"
        try:
            with SQLConnector(self.uri) as session:
                c = session.connection_db.cursor()
                c.execute(self.lock_timeout)
                c.execute(self.statement_timeout)
                c.execute(query)
            return True, f" OK:: REINDEX {index_name} (CONCURRENTLY={concurrently})"
        except psycopg.Error as e:
            return False, f" KO:: REINDEX {index_name}: {e}"

    def table_exists(self, table_name: str) -> bool:
        """
        Checks if a table exists in the database.

        Args:
            table_name (str): The name of the table.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        query = sql.SQL(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = %s
            )
            """
        )
        with SQLConnector(self.uri) as session:
            c = session.connection_db.cursor()
            c.execute(query, [table_name])
            result = c.fetchone()
        return result[0]
