"""Console script for pg_reindex."""
import argparse
import sys
import logging
from logging.handlers import RotatingFileHandler
import textwrap
from psycopg import conninfo
from .pg_reindex import Reindex
from .history import History


def set_logger(
    debug: bool = False, log_dir: str = None
) -> (logging.Logger, logging.Logger):
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
    if log_dir is not None:
        log_file_general = f"{log_dir}/pg_reindex.log"
        file_handler = RotatingFileHandler(log_file_general, "a", 1000000, 1)
        file_handler.setFormatter(formatter)
        global_logger.addHandler(file_handler)
    return global_logger


def set_uri(
    host: str = None, port: int = None, user: str = None, database: str = None
) -> str:
    if host is None:
        host = "127.0.0.1"
    if port is None:
        port = 5432
    if user is None:
        user = "postgres"
    if database is None:
        database = "postgres"
    uri = conninfo.make_conninfo(
        host=host, port=port, user=user, dbname=database, connect_timeout=10
    )
    return uri


def main():
    """Console script for pg_reindex."""
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
    parser.add_argument(
        "-l",
        "--log-dir",
        dest="log_dir",
        help="log dir",
        default="/tmp",
    )
    parser.add_argument("-H", "--host", dest="host", help="IP/hostname")
    parser.add_argument("-p", "--port", dest="port", help="port")
    parser.add_argument("-U", "--username", dest="user", help="User name")
    parser.add_argument("-d", "--database", dest="database", help="Database name")
    parser.add_argument(
        "--version", dest="display_version", help="Display version", action="store_true"
    )
    parser.add_argument(
        "--debug", dest="debug", help="Debug extended display", action="store_true"
    )
    parser.add_argument(
        "-L",
        "--lock-timeout",
        dest="lock_timeout",
        help="Time in seconds to wait for lock (default=120)",
        default=120,
    )
    parser.add_argument(
        "-s",
        "--statement-timeout",
        dest="statement_timeout",
        help="Time in seconds to wait for reindex command (no timeout)",
        default=0,
    )
    parser.add_argument(
        "-S",
        "--schema",
        dest="schemas",
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
        "-C", "--concurrently", dest="concurrently", action="store_true", help=""
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
    args = parser.parse_args()

    logger = set_logger(args.debug, args.log_dir)
    uri = set_uri(
        host=args.host, port=args.port, user=args.user, database=args.database
    )

    if args.with_history is False:
        History(args.log_dir, args.debug)

    if args.indexes is not None:
        logger.debug(f"Indexes to rebuild: {args.indexes}")
        pass
    elif args.tables is not None:
        logger.debug(f"Tables to reindex: {args.tables}")
        # work = Reindex(
        #    uri, args.log_dir, dry_run=args.dry_run, debug=args.debug, logger=logger
        # )
        # work.reindex_table_job(args.tables)
    elif args.schemas is not None:
        logger.debug(f"Schemas to reindex: {args.schemas}")
    else:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
