#!/usr/bin/env python

"""Tests for `pg_reindex` package."""

import pg_reindex.cli
import pytest


def test_set_conninfo():
    uri = pg_reindex.cli.set_conninfo()
    assert uri == 'host=127.0.0.1 port=5432 user=postgres dbname=postgres'
