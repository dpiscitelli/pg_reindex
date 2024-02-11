"""Console script for pg_reindex."""
import argparse
import sys
import os
import logging
from logging.handlers import RotatingFileHandler
import textwrap
from psycopg import conninfo
from .pg_reindex import Reindex
from .history import History


def set_logger(debug: bool = False, log_file: str = None) -> logging.Logger:
    # General log Error
    global_logger = logging.getLogger("global_log")
    if debug is True:
        global_logger.setLevel(logging.DEBUG)
    else:
        global_logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    global_logger.addHandler(stream_handler)
    if log_file is not None:
        file_handler = RotatingFileHandler(log_file, "a", 1000000, 1)
        file_handler.setFormatter(formatter)
        global_logger.addHandler(file_handler)
    return global_logger


def get_params_list(opt_param):
    if (
        isinstance(opt_param, list)
        and len(opt_param) == 1
        and os.path.isfile(opt_param[0])
    ):
        with open(opt_param[0], "r") as file:
            return file.read().splitlines()
    else:
        return opt_param


def main():
    desc = """Description: Script to rebuild indexes with criteria"""
    documentation = textwrap.dedent(
        """
            Examples of usages:\n
            *******************\n
            TODO\n
              \n"""
    )
    parser = argparse.ArgumentParser(
        description=desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=documentation,
    )
    parser.add_argument("-l", "--log-file", dest="log_file", help="log file")
    parser.add_argument(
        "-H", "--host", dest="host", default="127.0.0.1", help="IP/hostname"
    )
    parser.add_argument("-p", "--port", dest="port", default=5432, help="port")
    parser.add_argument(
        "-U", "--username", dest="user", default="postgres", help="User name"
    )
    parser.add_argument(
        "-d", "--database", dest="database", default="postgres", help="Database name"
    )
    parser.add_argument(
        "--version", dest="display_version", help="Display version", action="store_true"
    )
    parser.add_argument(
        "--debug", dest="debug", help="Debug extended display", action="store_true"
    )
    parser.add_argument(
        "-S",
        "--schema",
        dest="schemas",
        nargs="*",
        action="extend",
        help="Tables to reindex",
    )
    parser.add_argument(
        "-T",
        "--table",
        dest="tables",
        nargs="*",
        action="extend",
        help="Tables to reindex",
    )
    parser.add_argument(
        "-I",
        "--index",
        dest="indexes",
        nargs="*",
        action="extend",
        help="Tables to reindex",
    )
    parser.add_argument(
        "--lock-timeout",
        dest="lock_timeout",
        help="Time in seconds to wait for lock (default 2 min)",
        default=120,
    )
    parser.add_argument(
        "--statement-timeout",
        dest="statement_timeout",
        help="Time in seconds to wait for reindex command (default 5 hours)",
        default=18000,
    )
    parser.add_argument(
        "--concurrently", dest="concurrently", action="store_true", help=""
    )
    parser.add_argument("--dry-run", dest="dry_run", action="store_true", help="")
    parser.add_argument(
        "--with-history",
        dest="with_history",
        action="store_true",
        default="False",
        help="",
    )
    parser.add_argument(
        "--historydb", dest="historydb", default="/tmp/pg_reindex.db", help=""
    )
    parser.add_argument(
        "--get-history-errors",
        dest="history_errors",
        action="store_true",
        default="False",
        help="",
    )
    parser.add_argument(
        "--resume",
        dest="resume",
        action="store_true",
        default="False",
        help="",
    )
    parser.add_argument("-j", "--jobs", dest="jobs", default=1, help="")
    args = parser.parse_args()

    logger = set_logger(args.debug, args.log_file)
    uri = conninfo.make_conninfo(
        host=args.host,
        port=args.port,
        user=args.user,
        dbname=args.database,
        connect_timeout=10,
    )

    work = Reindex(uri, args)
    if args.indexes is not None:
        indexes = get_params_list(args.indexes)
        logger.debug(f"Indexes to rebuild: {indexes}")
        work.reindex_index_job(indexes)
    elif args.tables is not None:
        tables = get_params_list(args.tables)
        logger.debug(f"Tables to reindex: {tables}")
        work.reindex_table_job(tables)
    elif args.schemas is not None:
        schemas = get_params_list(args.schemas)
        logger.debug(f"Schemas to reindex: {schemas}")
        work.reindex_schema_job(args.schemas)
    else:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
