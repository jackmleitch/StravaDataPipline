import csv
import boto3
import configparser
import requests
from typing import Dict
from datetime import datetime

from utilities.mysql_utils import connect_mysql
from utilities.strava_api_utils import connect_strava, convert_strava_start_date
from utilities.s3_utils import connect_s3


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


columns_to_extract = [
    "id",
    "name",
    "distance",
    "moving_time",
    "elapsed_time",
    "total_elevation_gain",
    "type",
    "workout_type",
    "start_date",
    "timezone",
    "location_country",
    "achievement_count",
    "kudos_count",
    "comment_count",
    "athlete_count",
    "start_latlng",
    "end_latlng",
    "average_speed",
    "max_speed",
    "average_cadence",
    "average_temp",
    "average_heartrate",
    "max_heartrate",
    "suffer_score",
]

if __name__ == "__main__":
    # get last datetime of data extraction
    mysql_conn = connect_mysql()
    get_last_updated_query = """
        SELECT COALESCE(MAX(LastUpdated), '1900-01-01')
        FROM last_extracted;"""
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(get_last_updated_query)
    result = mysql_cursor.fetchone()
    last_updated_warehouse = datetime.strptime(result[0], "%Y-%m-%d %H:%M:%S")

    # connect to Strava API and get data
    header = connect_strava()
    all_activities = []
    activity_num = 1
    # while activity has not been extracted yet
    while True:
        response_json = make_strava_api_request(header, activity_num)
        date = response_json["start_date"]
        converted_date = convert_strava_start_date(date)
        if converted_date > last_updated_warehouse:
            activity = []
            for col in columns_to_extract:
                activity.append(response_json[col])
            all_activities.append(activity)
            activity_num += 1
        else:
            break

    # save extracted data to .csv file
    todays_date = datetime.today().strftime("%Y_%m_%d")
    export_file = f"strava_data/{todays_date}_export_file.csv"
    with open(export_file, "w") as fp:
        csvw = csv.writer(fp, delimiter="|")
        csvw.writerows(all_activities)
    print("Strava data extracted from API!")

    # upload .csv file to s3
    s3 = connect_s3()
    s3.upload_file(export_file, "strava-data-pipeline", export_file)
    print("Strava data uploaded to s3 bucket!")

    # update last extraction date in MySQL database
    update_last_updated_query = """
        INSERT INTO last_extracted (LastUpdated)
        VALUES (%s);"""
    mysql_cursor = mysql_conn.cursor()
    todays_datetime = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    mysql_cursor.execute(update_last_updated_query, todays_datetime)
    mysql_conn.commit()
    print("Extraction datetime added to MySQL database!")
