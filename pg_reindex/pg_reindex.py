import logging
from pg_reindex.pg_command import PGCommand


class Reindex:
    def __init__(self, uri, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.command = PGCommand(uri)

    def reindex_table(self, table, dry_run=False):
        schema_name, table_name = table.split(".")
        indexes = self.command.get_indexes(schema_name, table_name, "%")
        for i in indexes:
            self.logger.info(f"Table {table}: rebuild {i[0]}.{i[2]}")
            if dry_run is True:
                status, message = True, "Reindex (dry run !)"
            else:
                status, message = self.command.rebuild_index(f"{i[0]}.{i[2]}")
            if status is True:
                self.logger.info(f"{message}")
            else:
                self.logger.error(f"{message}")
