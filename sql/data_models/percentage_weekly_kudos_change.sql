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