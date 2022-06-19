# Strava Data EtLT Pipline
**:arrows_counterclockwise: :running: EtLT of my own Strava data using the Strava API, MySQL, Python, S3, and Redshift**

## [Data Extraction](https://github.com/jackmleitch/StravaDataPipline/blob/master/src/extract_strava_data.py) 
My own personal Strava activity data is first **ingested incrementally** using the [Strava API](https://developers.strava.com) and 
loaded into an **S3 bucket**. On each ingestion run, we query a MySQL database to get the date of the last extraction:

```python 
def get_date_of_last_warehouse_update() -> Tuple[datetime, str]:
    """
    Get the datetime of last time data was extracted from Strava API
    by querying MySQL database and also return current datetime.
    """
    mysql_conn = connect_mysql()
    get_last_updated_query = """
        SELECT COALESCE(MAX(LastUpdated), '1900-01-01')
        FROM last_extracted;"""
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(get_last_updated_query)
    result = mysql_cursor.fetchone()
    last_updated_warehouse = datetime.strptime(result[0], "%Y-%m-%d %H:%M:%S")
    current_datetime = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    return last_updated_warehouse, current_datetime
```

We then make repeated calls to the REST API using the `requests` library until we have all activity data between now and `last_updated_warehouse`. We include a `time.sleep()` command to comply with Strava's set rate limit of 100 requests/15 minutes. We also include `try: except:` blocks to combat 
missing data on certain activities. 
```python 
def make_strava_api_request(
    header: Dict[str, str], activity_num: int = 1
) -> Dict[str, str]:
    """Use Strava API to get recent page of new data."""
    param = {"per_page": 1, "page": activity_num}
    api_response = requests.get(
        "https://www.strava.com/api/v3/athlete/activities", headers=header, params=param
    ).json()
    response_json = api_response[0]
    return response_json
    
def extract_strava_activities(last_updated_warehouse: datetime) -> List[List]:
    """Connect to Strava API and get data up until last_updated_warehouse datetime."""
    header = connect_strava()
    all_activities = []
    activity_num = 1
    # while activity has not been extracted yet
    while True:
        # Strava has a rate limit of 100 requests every 15 mins
        if activity_num % 75 == 0:
            print("Rate limit hit, sleeping for 15 minutes...")
            time.sleep(15 * 60)
        try:
            response_json = make_strava_api_request(header, activity_num)
        # rate limit has exceeded, wait 15 minutes
        except KeyError:
            print("Rate limit hit, sleeping for 15 minutes...")
            time.sleep(15 * 60)
            response_json = make_strava_api_request(header, activity_num)
        date = response_json["start_date"]
        if date > last_updated_warehouse:
            activity = parse_api_output(response_json)
            all_activities.append(activity)
            activity_num += 1
        else:
            break
    return all_activities
```

Before exporting the data locally into a flat pipe-delimited `.csv` file, we perform a few minor transformations such as formatting dates and timezone columns. After we save the data, it is then uploaded to an S3 bucket for later loading into the data warehouse.
```python
def save_data_to_csv(all_activities: List[List]) -> str:
    """Save extracted data to .csv file."""
    todays_date = datetime.today().strftime("%Y_%m_%d")
    export_file_path = f"strava_data/{todays_date}_export_file.csv"
    with open(export_file_path, "w") as fp:
        csvw = csv.writer(fp, delimiter="|")
        csvw.writerows(all_activities)
    return export_file_path

def upload_csv_to_s3(export_file_path: str) -> None:
    """Upload extracted .csv file to s3 bucket."""
    s3 = connect_s3()
    s3.upload_file(export_file_path, "strava-data-pipeline", export_file_path)
```
Finally, we execute a query to update the MySQL database on the last date of extraction.
```python
def save_extraction_date_to_database(current_datetime: datetime) -> None:
    """Update last extraction date in MySQL database to todays datetime."""
    mysql_conn = connect_mysql()
    update_last_updated_query = """
        INSERT INTO last_extracted (LastUpdated)
        VALUES (%s);"""
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(update_last_updated_query, current_datetime)
    mysql_conn.commit()
```

## [Data Loading](https://github.com/jackmleitch/StravaDataPipline/blob/master/src/copy_to_redshift.py)
Once the data is loaded into the S3 data lake it is then loaded into our **Redshift** data warehouse. We do this by first loading the data from the S3 bucket into a staging table with the same schema as our production table. We then check for duplicates between the two tables using the 'id' primary key and if any are found they are deleted from the production table. The data from the staging table is then fully inserted into the production table. 
```python 
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
```

## [Data Transformations](https://github.com/jackmleitch/StravaDataPipline/blob/master/sql/)
Now the data has been ingested into the data warehouse, the next step in the pipeline is data transformations. Data transformations in this case include both noncontextual manipulation of the data and modeling of the data with context and logic in mind. The benefit of using the ELT methodology instead of the ETL framework, in this case, is that it gives us (/the end-user) the freedom in transforming the data the way we need as opposed to having a fixed data model that we cannot change (easily). In my case, I am connecting my Redshift data warehouse to Tableau building out a dashboard. We can, for example, perform simple queries to extract monthly statistics:
```sql 
SELECT EXTRACT(MONTH FROM start_date) AS activity_month,
    ROUND(SUM(distance)/1609) AS total_miles_ran,
    ROUND(SUM(moving_time)/(60*60)) AS total_running_time_hours,
    ROUND(SUM(total_elevation_gain)) AS total_elevation_gain_meters,
    ROUND(SUM(athlete_count)) AS total_people_ran_with,
    ROUND(AVG(athlete_count)) AS average_people_ran_with
FROM public.strava_activity_data
WHERE type='Run'
GROUP BY activity_month
ORDER BY activity_month;
```
We can also build more complicated data models. For example, we can get the week-by-week percentage change in total weekly kudos broken down by workout type:
```sql
WITH weekly_kudos_count AS (
  SELECT DATE_PART('week', start_date) AS week_of_year, 
    workout_type, 
    SUM(kudos_count) AS total_kudos
  FROM public.strava_activity_data
  WHERE type = 'Run' AND DATE_PART('year', start_date) = '2022'
  GROUP BY week_of_year, workout_type
),

weekly_kudos_count_lag AS (
  SELECT *, 
    LAG(total_kudos) OVER(PARTITION BY workout_type ORDER BY week_of_year) 
        AS previous_week_total_kudos
  FROM weekly_kudos_count
)

SELECT *, 
    COALESCE(ROUND(((total_kudos - previous_week_total_kudos)/previous_week_total_kudos)*100),0)
        AS percent_kudos_change
FROM weekly_kudos_count_lag;
```

## Data Visualization
With the data transformations done we were then able to build out an interactive dashboard using Tableau that updates automatically when new data gets intgested to the data warehouse (which is daily). The dashboard I created was built to investigate how Kudos on my own Strava activities changes over time and location. After building this project I shut down the Redshift server as to not incur any costs but a screenshot of the dashboard can be seen below.
![dashboard](https://github.com/jackmleitch/StravaDataPipline/blob/master/images/dashboard.png)

## [Unit Testing](https://github.com/jackmleitch/StravaDataPipline/tree/master/tests)
Unit testing was performed using PyTest and all tests can be found in the tests directory. For example, below we see a unit test to test the `make_strava_api_request` function. It asserts that a dictionary response is received and also that the response contains an 'id' key that is an integer. 
```python
@pytest.mark.filterwarnings("ignore::urllib3.exceptions.InsecureRequestWarning")
def test_make_strava_api_request():
    header = connect_strava()
    response_json = make_strava_api_request(header=header, activity_num=1)
    assert "id" in response_json.keys(), "Response dictionary does not contain id key."
    assert isinstance(response_json, dict), "API should respond with a dictionary."
    assert isinstance(response_json["id"], int), "Activity ID should be an integer."

```
