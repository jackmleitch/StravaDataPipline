from src.utilities.redshift_utils import connect_redshift


def build_data_model(sql_script_path: str) -> None:
    """Execute sql query to build data model."""
    rs_conn = connect_redshift()
    cursor = rs_conn.cursor()
    sql_file = open(sql_script_path, "r")
    cursor.execute(sql_file.read())
    cursor.close()


if __name__ == "__main__":
    sql_script_path = "sql/data_models/build_monthly_data_model.sql"
    build_data_model(sql_script_path)
