from src.utilities.redshift_utils import connect_redshift, get_s3_and_iam_details


def copy_to_redshift(
    table_name: str, redshift_connection, s3_file_path: str, role_string: str
) -> None:
    """Copy data from s3 into Redshift using staging table to remove duplicates."""

    # write queries to execute on redshift
    create_temp_table = f"CREATE TEMP TABLE staging_table (LIKE {table_name});"
    sql_copy_to_temp = f"COPY staging_table FROM {s3_file_path} iam_role {role_string};"
    # if id already exists in table, we remove it and add new id record during load
    delete_from_table = f"DELETE FROM {table_name} USING staging_table WHERE {table_name}.id = staging_table.id;"
    insert_into_table = f"INSERT INTO {table_name} SELECT * FROM staging_table;"
    drop_temp_table = "DROP TABLE staging_table;"

    # execute queries
    cur = rs_conn.cursor()
    cur.execute(create_temp_table)
    cur.execute(sql_copy_to_temp)
    cur.execute(delete_from_table)
    cur.execute(insert_into_table)
    cur.execute(drop_temp_table)
    rs_conn.commit()


if __name__ == "__main__":
    table_name = "public.strava_activity_data"
    rs_conn = connect_redshift()
    s3_file_path, role_string = get_s3_and_iam_details()
    copy_to_redshift(table_name, rs_conn, s3_file_path, role_string)
