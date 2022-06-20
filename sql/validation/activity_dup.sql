WITH activity_dups AS
(
  SELECT id, Count(*)
  FROM public.strava_activity_data
  GROUP BY id
  HAVING COUNT(*) > 1
)
SELECT COUNT(*)
FROM activity_dups;