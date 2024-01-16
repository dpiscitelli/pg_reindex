import logging
import psycopg
from psycopg import sql
from pg_reindex.sql_connector import SQLConnector


class PGCommand:
    def __init__(
        self,
        uri,
        lock_timeout=120,
        statement_timeout=18000,
        debug=False,
    ):
        self.uri = uri
        self.logger = logging.getLogger("global_log")
        self.debug = debug
        self.lock_timeout = f"SET LOCK_TIMEOUT = '{lock_timeout}s'"
        self.statement_timeout = f"SET STATEMENT_TIMEOUT = '{statement_timeout}s'"

    def get_indexes(self, schema_name, table_name, index_pattern="%"):
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
            WHERE (c.relkind = ANY (ARRAY['r'::"char", 'm'::"char", 'p'::"char"])) AND (i.relkind = ANY (ARRAY['i'::"char", 'I'::"char"]))
            AND n.nspname = %s
            AND c.relname = %s
            AND i.relname SIMILAR TO %s"""
        )
        with SQLConnector(self.uri) as session:
            c = session.connection_db.cursor()
            c.execute(query, [schema_name, table_name, index_pattern])
            index_list = c.fetchall()
            if self.debug is True:
                self.logger.debug(f"PGCommand.get_indexes:: Index list: {index_list}")
            return index_list

    def get_tables_from_schemas(self, schemas):
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
