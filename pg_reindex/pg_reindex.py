import logging
from .pg_command import PGCommand
from .history import History


class Reindex:
    def __init__(self, uri, log_dir, dry_run=False, debug=False, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.command = PGCommand(uri, debug)
        self.dry_run = dry_run
        self.history = History(log_dir, debug)

    def reindex_table_unit(self, schema_name, table_name):
        indexes = self.command.get_indexes(schema_name, table_name, "%")
        for i in indexes:
            self.logger.info(f"Table {schema_name}.{table_name}: rebuild {i[0]}.{i[2]}")
            if self.dry_run is True:
                status, message = True, "Reindex (dry run !)"
            else:
                status, message = self.command.rebuild_index(f"{i[0]}.{i[2]}")
            if status is True:
                self.logger.info(f"{message}")
            else:
                self.logger.error(f"{message}")
            self.history.push_history(
                f"TABLE::{schema_name}.{table_name}::INDEX::{i[0]}.{i[2]}"
            )

    def reindex_table_job(self, tables):
        for t in tables:
            schema_name, table_name = t.split(".")
            self.reindex_table_unit(schema_name, table_name)

    def reindex_table(self, table_pattern):
        schema_name, table_name = table_pattern.split(".")
        self.logger.debug(
            f"Reindex.reindex_tables:: schema={schema_name}, table pattern={table_name}"
        )
        tables = self.command.get_tables(schema_name, table_name)
        self.logger.debug(f"Reindex.reindex_tables:: tables to process => {tables}")
