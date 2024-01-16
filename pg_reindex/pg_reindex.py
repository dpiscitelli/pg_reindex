import logging
from .pg_command import PGCommand
from concurrent.futures import ThreadPoolExecutor
from .history import History


class Reindex:
    def __init__(self, uri, opts):
        self.logger = logging.getLogger("global_log")
        self.command = PGCommand(
            uri,
            lock_timeout=opts.lock_timeout,
            statement_timeout=opts.statement_timeout,
            debug=opts.debug,
        )
        self.dry_run = opts.dry_run
        self.concurrently = opts.concurrently
        self.jobs = opts.jobs
        # self.history = History(log_dir, debug)

    def reindex_table_unit(self, table):
        schema_name, table_name = table.split(".")
        indexes = self.command.get_indexes(schema_name, table_name, "%")
        for i in indexes:
            self.logger.info(f"Table {schema_name}.{table_name}: rebuild {i[0]}.{i[2]}")
            if self.dry_run is True:
                status, message = True, "Reindex (dry run !)"
            else:
                status, message = self.command.rebuild_index(
                    f"{i[0]}.{i[2]}", concurrently=self.concurrently
                )
            if status is True:
                self.logger.info(f"{message}")
            else:
                self.logger.error(f"{message}")
            # self.history.push_history(
            #    f"TABLE::{schema_name}.{table_name}::INDEX::{i[0]}.{i[2]}"
            # )
        return True

    def reindex_index_job(self, indexes):
        for i in indexes:
            if self.dry_run is True:
                status, message = True, "Reindex (dry run !)"
            else:
                status, message = self.command.rebuild_index(i)
            if status is True:
                self.logger.info(f"{message}")
            else:
                self.logger.error(f"{message}")
        return True

    def reindex_table_job(self, tables):
        with ThreadPoolExecutor(self.jobs) as executor:
            executor.map(self.reindex_table_unit, tables)
        return True

    def reindex_schema_job(self, schemas):
        tables = self.command.get_tables_from_schemas(schemas)
        self.logger.debug(f"Reindex.reindex_schema_job:: Tables to reindex {tables}")
        self.reindex_table_job(tables)
        return True
