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
        self.jobs = int(opts.jobs)
        self.with_history = opts.with_history
        self.historydb = opts.historydb
        self.debug = opts.debug
        self.database_name = opts.database
        self.resume = opts.resume

    def reindex_table_unit(self, table):
        schema_name, table_name = table.split(".")
        indexes = self.command.get_indexes(schema_name, table_name, "%")
        for i in indexes:
            index_name = f"{i[0]}.{i[2]}"
            if self.dry_run is True:
                status, message = True, "Reindex (dry run !)"
            else:
                if self.resume is True and self.with_history:
                    history = History(self.historydb, self.debug)
                    status = history.get_reindex_status(self.database_name, index_name)
                    if status is not None and status == 1:
                        self.logger.info(f"Index already rebuild: {index_name}")
                        continue
                self.logger.info(
                    f"Table {schema_name}.{table_name}: rebuild {index_name}"
                )
                status, message = self.command.rebuild_index(
                    index_name, concurrently=self.concurrently
                )
            if status is True:
                self.logger.info(f"{message}")
            else:
                self.logger.error(f"{message}")
            if self.with_history:
                history = History(self.historydb, self.debug)
                history.set_or_update_history(
                    self.database_name,
                    schema_name,
                    table_name,
                    index_name,
                    status,
                    message,
                )
        return status

    def reindex_index_job(self, indexes):
        for i in indexes:
            if self.dry_run is True:
                status, message = True, "Reindex (dry run !)"
            else:
                if self.resume is True and self.with_history:
                    history = History(self.historydb, self.debug)
                    status = history.get_reindex_status(self.database_name, i)
                    if status is not None and status == 1:
                        self.logger.info(f"Index already rebuild: {i}")
                        continue
                status, message = self.command.rebuild_index(i)
                if self.with_history:
                    history = History(self.historydb, self.debug)
                    history.set_or_update_history(
                        self.database_name, None, None, i, status, message
                    )
            if status is True:
                self.logger.info(f"{message}")
            else:
                self.logger.error(f"{message}")
        return status

    def reindex_table_job(self, tables):
        with ThreadPoolExecutor(self.jobs) as executor:
            nb = 0
            for result in executor.map(self.reindex_table_unit, tables):
                nb += 1
                self.logger.info(f"Job {nb}: {result}")
        return True

    def reindex_schema_job(self, schemas):
        tables = self.command.get_tables_from_schemas(schemas)
        self.logger.debug(f"Reindex.reindex_schema_job:: Tables to reindex {tables}")
        self.reindex_table_job(tables)
        return True
