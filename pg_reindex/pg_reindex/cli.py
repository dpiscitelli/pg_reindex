"""Console script for pg_reindex."""
import argparse
import sys


def main():
    """Console script for pg_reindex."""
    parser = argparse.ArgumentParser()
    parser.add_argument('_', nargs='*')
    args = parser.parse_args()

    print("Arguments: " + str(args._))
    print("Replace this message by putting your code into "
          "pg_reindex.cli.main")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
