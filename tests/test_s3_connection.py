import pytest
from src.utilities.s3_utils import connect_s3


def test_connect_s3():
    s3 = connect_s3()
    assert s3, "S3 connection failed!"
