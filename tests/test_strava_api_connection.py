import pytest
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
