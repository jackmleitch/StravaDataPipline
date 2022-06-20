import os
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

os.chdir('/Users/Jack/Documents/projects/StravaDataPipeline/')

schedule_interval = '@weekly' 
start_date = days_ago(1)

default_args = {"owner": "airflow", "depends_on_past": False, "retries": 1}

with DAG(
    dag_id='elt_strava_pipeline',
    description ='Strava data EtLT pipeline',
    schedule_interval=schedule_interval,
    default_args=default_args,
    start_date=start_date,
    catchup=True,
    max_active_runs=1,
    tags=['StravaELT'],
) as dag:

    extract_strava_data = BashOperator(
        task_id = 'extract_strava_data',
        bash_command = "src/extract_strava_data.py",
        dag = dag,
    )
    extract_strava_data.doc_md = 'Extract Strava data and store as csv in S3 bucket.'
    
    copy_to_redshift_staging = BashOperator(
        task_id = 'copy_to_redshift_staging',
        bash_command = "src/copy_to_redshift_staging.py",
        dag = dag,
    )
    copy_to_redshift_staging.doc_md = 'Copy S3 csv file to Redshift staging table.'

    validate_staging_data_dup = BashOperator(
        task_id = 'validate_staging_data_dup',
        bash_command = "python src/validator.py sql/validation/activity_dup.sql sql/validation/activity_dup_zero.sql equals warn",
        dag = dag,
    )
    validate_staging_data_dup.doc_md = 'Validate data: check for duplicates.'

    validate_staging_data_weekly_activity_count = BashOperator(
        task_id = 'validate_staging_data_weekly_activity_count',
        bash_command = "python src/validator.py sql/validation/weekly_activity_count_zscore.sql sql/validation/zscore_90_twosided.sql greater_equals warn",
        dag = dag,
    )
    validate_staging_data_weekly_activity_count.doc_md = 'Validate data: z-test weekly activity count.'

    validate_staging_data_weekly_kudos_avg = BashOperator(
        task_id = 'validate_staging_data_weekly_kudos_avg',
        bash_command = "python src/validator.py sql/validation/weekly_kudos_avg_zscore.sql sql/validation/zscore_90_twosided.sql greater_equals warn",
        dag = dag,
    )
    validate_staging_data_weekly_kudos_avg.doc_md = 'Validate data: z-test weekly kudos average'

    redshift_staging_to_production = BashOperator(
        task_id = 'redshift_staging_to_production',
        bash_command = "src/redshift_staging_to_production.py",
        dag = dag,
    )
    redshift_staging_to_production.doc_md = 'Insert redshift staging table into production and remove duplicates.'

    build_data_model = BashOperator(
        task_id = 'build_data_model',
        bash_command = "src/build_data_model.py",
        dag = dag,
    )
    build_data_model.doc_md = 'Build monthly statistics data model.'

extract_strava_data >> copy_to_redshift_staging
copy_to_redshift_staging >> validate_staging_data_dup
copy_to_redshift_staging >> validate_staging_data_weekly_activity_count
copy_to_redshift_staging >> validate_staging_data_weekly_kudos_avg
validate_staging_data_dup >> redshift_staging_to_production
validate_staging_data_weekly_activity_count >> redshift_staging_to_production
validate_staging_data_weekly_kudos_avg >> redshift_staging_to_production
redshift_staging_to_production >> build_data_model