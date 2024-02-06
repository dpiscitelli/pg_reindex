#!/usr/bin/env python
import pytest
import unittest
import sqlite3
from pg_reindex import pg_reindex


# Define a test case class 'TestDatabaseQuery' that inherits from 'unittest.TestCase'.
class Test(unittest.TestCase):
    # Define a method 'setUp' that is executed before each test.
    def setUp(self):
        # Create a database connection in memory and insert test data.
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()
        # Create an 'employees' table and insert test records.
        self.cursor.execute(
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, salary REAL)"
        )
        self.cursor.execute(
            "INSERT INTO employees (name, salary) VALUES ('Ylva Guiomar', 1800.0)"
        )
        self.cursor.execute(
            "INSERT INTO employees (name, salary) VALUES ('Scott Gregorius', 2100.0)"
        )
        self.conn.commit()

    # Define a method 'tearDown' that is executed after each test.
    def tearDown(self):
        # Close the database cursor and the database connection.
        self.cursor.close()
        self.conn.close()

    # Define a test method 'test_database_query' to test a database query.
    def test_database_query(self):
        # Execute a SQL query to select employee names and salaries, ordered by name.
        self.cursor.execute("SELECT name, salary FROM employees ORDER BY name")
        results = self.cursor.fetchall()

        # Define the expected results as a list of tuples.

        # Assert that the results match the expected results.
        self.assertEqual(results, expected_results)

    # assert 'GitHub' in BeautifulSoup(response.content).title.string
