import requests
import configparser
import urllib3

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
