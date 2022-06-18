import pytest
from datetime import datetime

from src.extract_strava_data import make_strava_api_request, extract_strava_activities
from src.utilities.strava_api_utils import connect_strava


@pytest.mark.filterwarnings("ignore::urllib3.exceptions.InsecureRequestWarning")
def test_connect_strava():
    header = connect_strava()
    assert isinstance(
        header, dict
    ), "Header returned is not dictionary. Connection has likley failed."
    assert (
        header["Authorization"].endswith("Bearer ") == False
    ), "Access token not found."


@pytest.mark.filterwarnings("ignore::urllib3.exceptions.InsecureRequestWarning")
def test_make_strava_api_request():
    header = connect_strava()
    response_json = make_strava_api_request(header=header, activity_num=1)
    assert isinstance(response_json, dict), "API should respond with a dictionary."
    assert isinstance(response_json["id"], int), "Activity ID should be an integer."
