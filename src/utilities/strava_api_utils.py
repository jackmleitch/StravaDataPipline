import requests
import configparser
import urllib3
import re

from typing import Dict
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def connect_strava() -> Dict[str, str]:
    """Get the Strava API connection info and return header."""
    # get strava api info
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    auth_url = parser.get("strava_api_config", "auth_url")
    client_id = parser.get("strava_api_config", "client_id")
    client_secret = parser.get("strava_api_config", "client_secret")
    refresh_token = parser.get("strava_api_config", "refresh_token")

    # connect to API
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
        "f": "json",
    }
    res = requests.post(auth_url, data=payload, verify=False)
    access_token = res.json()["access_token"]
    header = {"Authorization": "Bearer " + access_token}
    return header


def convert_strava_start_date(date: str) -> datetime:
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    converted_date = datetime.strptime(date, date_format)
    return converted_date


def parse_api_output(response_json: dict) -> list:
    """Parse output from Strava API."""
    activity = []
    cols_to_extract = [
        "id",
        "name",
        "distance",
        "moving_time",
        "elapsed_time",
        "total_elevation_gain",
        "type",
        "workout_type",
        "location_country",
        "achievement_count",
        "kudos_count",
        "comment_count",
        "athlete_count",
        "average_speed",
        "max_speed",
        "average_cadence",
        "average_temp",
        "average_heartrate",
        "max_heartrate",
        "suffer_score",
    ]
    for col in cols_to_extract:
        try:
            activity.append(response_json[col])
        # if col is not found in API repsonse
        except KeyError:
            activity.append(None)
    try:
        start_date = convert_strava_start_date(response_json["start_date"])
        activity.append(start_date)
    except KeyError:
        activity.append(None)
    try:
        # remove timezone info
        timezone = response_json["timezone"]
        timezone = re.sub("[\(\[].*?[\)\]]", "", timezone)
        activity.append(timezone[1:])
    except KeyError:
        activity.append(None)
    try:
        start_latlng = response_json["start_latlng"]
        if len(start_latlng) == 2:
            lat, lng = start_latlng[0], start_latlng[1]
            activity.append(lat)
            activity.append(lng)
        else:
            activity.append(None)
            activity.append(None)
    except KeyError:
        activity.append(None)
        activity.append(None)
    return activity
