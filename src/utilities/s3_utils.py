import boto3
import configparser


def connect_s3():
    """Get the MySQL connection info and connect."""
    # load the aws_boto_credentials values
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    access_key = parser.get("aws_boto_credentials", "access_key")
    secret_key = parser.get("aws_boto_credentials", "secret_key")
    bucket_name = parser.get("aws_boto_credentials", "bucket_name")
    # connect to s3 bucket
    s3 = boto3.client(
        "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
    )
    return s3
