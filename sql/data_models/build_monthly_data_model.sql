CREATE TABLE IF NOT EXISTS activity_summary_monthly (
  activity_month numeric,
  total_miles_ran int,
  total_running_time_hours int,
  total_elevation_gain_meters int,
  total_people_ran_with int,
  avg_people_ran_with int, 
  avg_kudos real, 
  std_kudos real
);

TRUNCATE activity_summary_monthly;

INSERT INTO activity_summary_monthly
SELECT EXTRACT(MONTH FROM start_date) AS activity_month,
    ROUND(SUM(distance)/1609) AS total_miles_ran,
    ROUND(SUM(moving_time)/(60*60)) AS total_running_time_hours,
    ROUND(SUM(total_elevation_gain)) AS total_elevation_gain_meters,
    ROUND(SUM(athlete_count)) AS total_people_ran_with,
    ROUND(AVG(athlete_count)) AS avg_people_ran_with,
    ROUND(AVG(kudos_count), 1) AS avg_kudos,
    ROUND(STDDEV(kudos_count), 1) AS std_kudos
FROM public.strava_activity_data
WHERE type='Run'
GROUP BY activity_month
ORDER BY activity_month;