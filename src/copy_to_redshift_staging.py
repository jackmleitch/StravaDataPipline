import configparser

from src.utilities.redshift_utils import connect_redshift, get_s3_and_iam_details


def copy_to_redshift_staging(
    table_name: str, rs_conn, s3_file_path: str, role_string: str
) -> None:
    """Copy data from s3 into Redshift staging table."""
    # write queries to execute on redshift
    create_temp_table = f"CREATE TABLE staging_table (LIKE {table_name});"
    sql_copy_to_temp = f"COPY staging_table FROM {s3_file_path} iam_role {role_string};"

    # execute queries
    cur = rs_conn.cursor()
    cur.execute(create_temp_table)
    cur.execute(sql_copy_to_temp)
    rs_conn.commit()


if __name__ == "__main__":
    # get redshift table name
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    table_name = parser.get("aws_redshift_creds", "table_name")
    # copy s3 data to redshift staging table
    rs_conn = connect_redshift()
    s3_file_path, role_string = get_s3_and_iam_details()
    copy_to_redshift_staging(table_name, rs_conn, s3_file_path, role_string)
