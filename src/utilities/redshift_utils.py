import configparser
import psycopg2
from datetime import datetime
from typing import Tuple


def connect_redshift():
    """Connect to the redshift cluster."""
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    dbname = parser.get("aws_redshift_creds", "database")
    user = parser.get("aws_redshift_creds", "username")
    password = parser.get("aws_redshift_creds", "password")
    host = parser.get("aws_redshift_creds", "host")
    port = parser.get("aws_redshift_creds", "port")
    conn = psycopg2.connect(
        dbname=dbname, host=host, port=port, user=user, password=password
    )
    return conn


def get_s3_and_iam_details(
    date: datetime = datetime.today().strftime("%Y_%m_%d"),
) -> Tuple[str, str]:
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    account_id = parser.get("aws_boto_credentials", "account_id")
    iam_role = parser.get("aws_redshift_creds", "iam_role")
    bucket_name = parser.get("aws_boto_credentials", "bucket_name")
    s3_file_path = f"s3://{bucket_name}/strava_data/{date}_export_file.csv"
    role_string = f"arn:aws:iam::{account_id}:role/{iam_role}"
    return s3_file_path, role_string
