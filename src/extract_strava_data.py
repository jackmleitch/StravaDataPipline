import csv
import requests
import time

from typing import Dict, List
from datetime import datetime

from utilities.mysql_utils import connect_mysql
from utilities.strava_api_utils import (
    connect_strava,
    convert_strava_start_date,
    parse_api_output,
)
from utilities.s3_utils import connect_s3


def get_date_of_last_warehouse_update() -> datetime:
    """
    Get the datetime of last time data was extracted from Strava API
    by querying MySQL database.
    """
    mysql_conn = connect_mysql()
    get_last_updated_query = """
        SELECT COALESCE(MAX(LastUpdated), '1900-01-01')
        FROM last_extracted;"""
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(get_last_updated_query)
    result = mysql_cursor.fetchone()
    last_updated_warehouse = datetime.strptime(result[0], "%Y-%m-%d %H:%M:%S")
    return last_updated_warehouse


def make_strava_api_request(
    header: Dict[str, str], activity_num: int = 1
) -> Dict[str, str]:
    """Use Strava API to get recent page of new data."""
    param = {"per_page": 1, "page": activity_num}
    api_response = requests.get(
        "https://www.strava.com/api/v3/athlete/activities", headers=header, params=param
    ).json()
    response_json = api_response[0]
    return response_json


def extract_strava_activities(last_updated_warehouse: datetime) -> List[List]:
    """Connect to Strava API and get data up until last_updated_warehouse datetime."""
    header = connect_strava()
    all_activities = []
    activity_num = 1
    # while activity has not been extracted yet
    while True:
        # Strava has a rate limit of 100 requests every 15 mins
        if activity_num % 75 == 0:
            print("Rate limit hit, sleeping for 15 minutes...")
            time.sleep(15 * 60)
        try:
            response_json = make_strava_api_request(header, activity_num)
        # rate limit has exceeded, wait 15 minutes
        except KeyError:
            print("Rate limit hit, sleeping for 15 minutes...")
            time.sleep(15 * 60)
            response_json = make_strava_api_request(header, activity_num)
        date = response_json["start_date"]
        converted_date = convert_strava_start_date(date)
        if converted_date > last_updated_warehouse:
            activity = parse_api_output(response_json)
            all_activities.append(activity)
            activity_num += 1
        else:
            break
    return all_activities


def save_data_to_csv(all_activities: List[List]) -> str:
    """Save extracted data to .csv file."""
    todays_date = datetime.today().strftime("%Y_%m_%d")
    export_file_path = f"strava_data/{todays_date}_export_file.csv"
    with open(export_file_path, "w") as fp:
        csvw = csv.writer(fp, delimiter="|")
        csvw.writerows(all_activities)
    print("Strava data extracted from API!")
    return export_file_path


def upload_csv_to_s3(export_file_path: str) -> None:
    """Upload extracted .csv file to s3 bucket."""
    s3 = connect_s3()
    s3.upload_file(export_file_path, "strava-data-pipeline", export_file_path)
    print("Strava data uploaded to s3 bucket!")


def save_extraction_date_to_database() -> None:
    """Update last extraction date in MySQL database to todays datetime."""
    mysql_conn = connect_mysql()
    update_last_updated_query = """
        INSERT INTO last_extracted (LastUpdated)
        VALUES (%s);"""
    mysql_cursor = mysql_conn.cursor()
    todays_datetime = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    mysql_cursor.execute(update_last_updated_query, todays_datetime)
    mysql_conn.commit()
    print("Extraction datetime added to MySQL database!")


if __name__ == "__main__":
    last_updated_warehouse = get_date_of_last_warehouse_update()
    all_activities = extract_strava_activities(last_updated_warehouse)
    export_file_path = save_data_to_csv(all_activities)
    upload_csv_to_s3(export_file_path)
    save_extraction_date_to_database()
