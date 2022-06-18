# Strava Data ETL Pipline
**:arrows_counterclockwise: :running: ETL of my own Strava data using the Strava API, MySQL, Python, S3, and Redshift**

## [Data Ingestion](https://github.com/jackmleitch/StravaDataPipline/blob/master/src/extract_strava_data.py) 
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

After storing this data locally in a flat pipe-delimited `.csv` file, it is then uploaded to an S3 bucket for later loading into the data warehouse.
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
