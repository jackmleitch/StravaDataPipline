import pytest
from datetime import datetime
from src.utilities.mysql_utils import connect_mysql


def test_connect_mysql():
    conn = connect_mysql()
    assert conn, "Connection to MySQL not found."


def test_basic_query():
    conn = connect_mysql()
    test_query = """
        SELECT *
        FROM last_extracted;"""
    mysql_cursor = conn.cursor()
    mysql_cursor.execute(test_query)
    result = mysql_cursor.fetchone()
    assert result, "SELECT * query failed."


def test_last_extraction_table():
    conn = connect_mysql()
    get_earliest_update_query = """
        SELECT MIN(LastUpdated)
        FROM last_extracted;"""
    mysql_cursor = conn.cursor()
    mysql_cursor.execute(get_earliest_update_query)
    result = mysql_cursor.fetchone()[0]
    assert result == datetime(
        2022, 1, 1, 0, 0
    ), "First date in updates tables is incorrect."
