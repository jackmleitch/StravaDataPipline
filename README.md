# Strava Data ETL Pipline
**:arrows_counterclockwise: :running: ETL of my own Strava data using the Strava API, MySQL, Python, S3, and Redshift**

## Data Ingestion 
My own personal Strava activity data is first **ingested incrementally** using the [Strava API](https://developers.strava.com) and 
loaded into an **S3 bucket**. On each ingestion run, we query a MySQL database to get the date of the last extraction:

```python 
mysql_conn = connect_mysql()
get_last_updated_query = """
    SELECT COALESCE(MAX(LastUpdated), '1900-01-01')
    FROM last_extracted;"""
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute(get_last_updated_query)
result = mysql_cursor.fetchone()
last_updated_warehouse = datetime.strptime(result[0], "%Y-%m-%d %H:%M:%S")
```

We then make repeated calls to the REST API using the `requests` library until we have all activity data between now and `last_updated_warehouse`. 
```python 
header = connect_strava()
all_activities = []
activity_num = 1
# while activity has not been extracted yet
while True:
    response_json = make_strava_api_request(header, activity_num)
    date = response_json["start_date"]
    converted_date = convert_strava_start_date(date)
    if converted_date > last_updated_warehouse:
        activity = []
        for col in columns_to_extract:
            activity.append(response_json[col])
        all_activities.append(activity)
        activity_num += 1
    else:
        break
```

After storing this data locally in a flat pipe-delimited `.csv' file, it is then uploaded to an S3 bucket for later loading into the data warehouse.
```python
todays_date = datetime.today().strftime("%Y_%m_%d")
export_file = f"strava_data/{todays_date}_export_file.csv"
with open(export_file, "w") as fp:
    csvw = csv.writer(fp, delimiter="|")
    csvw.writerows(all_activities)
s3 = connect_s3()
s3.upload_file(export_file, "strava-data-pipeline", export_file)
```
Finally, we execute a query to update the MySQL database on the last date of extraction.
```python
update_last_updated_query = """
    INSERT INTO last_extracted (LastUpdated)
    VALUES (%s);"""
mysql_cursor = mysql_conn.cursor()
todays_datetime = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
mysql_cursor.execute(update_last_updated_query, todays_datetime)
mysql_conn.commit()
```
