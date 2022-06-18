import pytest
from datetime import datetime
from src.utilities.mysql_utils import connect_mysql
from src.extract_strava_data import get_date_of_last_warehouse_update


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


def test_get_date_of_last_warehouse_update():
    last_update, current_date = get_date_of_last_warehouse_update()

    assert isinstance(
        last_update, datetime
    ), "Last update date should be a datetime object."
    assert isinstance(current_date, str), "Last update date should be a string."
    current_datetime = datetime.strptime(current_date, "%Y-%m-%d %H:%M:%S")
    assert (
        current_datetime > last_update
    ), "Current date should be bigger than last update date in database"
