SELECT EXTRACT(YEAR FROM start_date) AS activity_year,
    ROUND(SUM(distance)/1609) AS total_miles_ran,
    ROUND(SUM(moving_time)/(60*60)) AS total_running_time_hours,
    ROUND(SUM(total_elevation_gain)) AS total_elevation_gain_meters,
    ROUND(SUM(athlete_count)) AS total_people_ran_with,
    ROUND(AVG(athlete_count)) AS average_people_ran_with
FROM public.strava_activity_data
WHERE type='Run'
GROUP BY activity_year
ORDER BY activity_year;