import pymysql
import configparser


def connect_mysql():
    """Get the MySQL connection info and connect."""
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    hostname = parser.get("mysql_config", "hostname")
    port = parser.get("mysql_config", "port")
    username = parser.get("mysql_config", "username")
    dbname = parser.get("mysql_config", "database")
    password = parser.get("mysql_config", "password")

    conn = pymysql.connect(
        host=hostname, user=username, password=password, db=dbname, port=int(port)
    )
    if conn is None:
        print("Error connecting to the MySQL database")
    else:
        print("MySQL connection established!")
    return conn
