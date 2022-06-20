import configparser

from src.utilities.redshift_utils import connect_redshift


def redshift_staging_to_production(table_name: str, rs_conn) -> None:
    """Copy data from Redshift staging table to production table."""
    # if id already exists in table, we remove it and add new id record during load
    delete_from_table = f"DELETE FROM {table_name} USING staging_table WHERE '{table_name}'.id = staging_table.id;"
    insert_into_table = f"INSERT INTO {table_name} SELECT * FROM staging_table;"
    drop_temp_table = "DROP TABLE staging_table;"
    # execute queries
    cur = rs_conn.cursor()
    cur.execute(delete_from_table)
    cur.execute(insert_into_table)
    cur.execute(drop_temp_table)
    rs_conn.commit()


if __name__ == "__main__":
    # get redshift table name
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    table_name = parser.get("aws_redshift_creds", "table_name")
    # copy redshift staging table to production table
    table_name = "public.strava_activity_data"
    rs_conn = connect_redshift()
    redshift_staging_to_production(table_name, rs_conn)
