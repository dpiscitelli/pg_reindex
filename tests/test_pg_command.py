#!/usr/bin/env python

import pytest
import unittest
import logging
from psycopg import sql, conninfo
from pg_reindex.pg_command import PGCommand


# Define a test case class 'TestDatabaseQuery' that inherits from 'unittest.TestCase'.
class TestPGCommand(unittest.TestCase):
    def setUp(self):
        self.uri = conninfo.make_conninfo(
            host="127.0.0.1",
            port=5432,
            user="lengow",
            dbname="lengow",
            password="lengow44",
            connect_timeout=10,
        )
        self.logger = logging.getLogger()
        self.command = PGCommand(self.uri)

    # Define a method 'tearDown' that is executed after each test.
    def tearDown(self):
        pass

    # Define a test method 'test_database_query' to test a database query.
    def test_get_indexes(self):
        expected_results = [
            ("catalog", "structure", "structure_pkey", "p", "btree"),
            ("catalog", "structure", "structure_common_account_id_idx", "i", "btree"),
            ("catalog", "structure", "structure_common_account_id_idx1", "i", "btree"),
            (
                "catalog",
                "structure",
                "structure_common_account_id_url_idx",
                "u",
                "btree",
            ),
        ]

        indexes = self.command.get_indexes("catalog", "structure", "%")
        self.assertEqual(indexes, expected_results)

    # assert 'GitHub' in BeautifulSoup(response.content).title.string
