SELECT workout_type, AVG(kudos_count) AS average_kudos, AVG(average_speed) AS average_speed
FROM public.strava_activity_data
WHERE type = 'Run'
GROUP BY workout_type
ORDER BY average_kudos DESC;