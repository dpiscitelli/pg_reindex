import logging
import psycopg
from psycopg import sql
from pg_reindex.sql_connector import SQLConnector


class PGCommand:
    def __init__(self, uri, debug=False, logger=None):
        self.uri = uri
        self.logger = logger or logging.getLogger(__name__)
        self.debug = debug

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
                a.amname AS index_method
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

    def get_tables(self, schema_pattern, table_pattern):
        query = sql.SQL(
            """
            SELECT
                n.nspname AS schemaname,
                c.relname AS tablename
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE (c.relkind = ANY (ARRAY['r'::"char", 'm'::"char", 'p'::"char"]))
            AND n.nspname SIMILAR TO %s
            AND c.relname SIMILAR TO %s
            """
        )
        try:
            with SQLConnector(self.uri) as session:
                c = session.connection_db.cursor()
                c.execute(query, [schema_pattern, table_pattern])
                return c.fetchall()
        except psycopg.Error as e:
            self.logger.error(f"Error: {e}")
            self.logger.error(f"Query: {e}")
            return False, e

    def rebuild_index(self, index_name, concurrently=False):
        query = sql.SQL(f"REINDEX INDEX %s")
        if concurrently is True:
            query = sql.SQL(f"REINDEX INDEX CONCURRENTLY %s")
        try:
            with SQLConnector(self.uri) as session:
                c = session.connection_db.cursor()
                c.execute(query, [index_name])
            return True, c.statusmessage
        except psycopg.Error as e:
            self.logger.error(f"INDEX: {index_name} :: {e}")
            return False, e
