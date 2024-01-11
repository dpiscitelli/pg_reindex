"""Console script for pg_reindex."""
import argparse
import sys
import logging
from logging.handlers import RotatingFileHandler
import textwrap
from psycopg import conninfo
from pg_reindex.pg_reindex import Reindex


def set_logger(debug: bool = False, log_file: str = None) -> logging.Logger:
    logger = logging.getLogger()
    if debug is True:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    if log_file is not None:
        file_handler = RotatingFileHandler(log_file, "a", 1000000, 1)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger


def set_conninfo(
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
        "--log-file",
        dest="log_file",
        help="log file",
        default="/tmp/pg_reindex.log",
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
        "-S",
        "--statement-timeout",
        dest="statement_timeout",
        help="Time in seconds to wait for reindex command (no timeout)",
        default=0,
    )
    parser.add_argument(
        "-t",
        "--table",
        nargs="*",
        action="extend",
        dest="tables",
        help="Tables to reindex",
    )
    parser.add_argument(
        "-C", "--concurrently", dest="concurrently", action="store_true", help=""
    )
    parser.add_argument("--dry-run", dest="dry_run", action="store_true", help="")
    args = parser.parse_args()

    logger = set_logger(args.debug, args.log_file)
    uri = set_conninfo(
        host=args.host, port=args.port, user=args.user, database=args.database
    )
    if args.tables is not None:
        logger.debug(f"Tables to reindex: {args.tables}")
        work = Reindex(uri)
        for t in args.tables:
            work.reindex_table(t, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
