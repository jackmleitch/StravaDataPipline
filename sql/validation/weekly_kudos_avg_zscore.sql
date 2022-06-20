with kudos_by_week AS (
  SELECT 
  	date_trunc('week', start_date::date) AS activity_week,
  	AVG(kudos_count) AS avg_weekly_kudos           
  FROM public.strava_activity_data
  GROUP BY activity_week
  ORDER BY activity_week
),

kudos_by_week_statistics AS (
  SELECT 
  	AVG(avg_weekly_kudos) AS avg_weekly_kudos,
  	STDDEV(avg_weekly_kudos) AS std_weekly_kudos
  FROM kudos_by_week
),

staging_table_avg_kudos AS (
  SELECT AVG(kudos_count) AS staging_avg_kudos
  FROM staging_table
),

weekly_avg_kudos_zscore AS (
  SELECT
  	s.staging_avg_kudos AS staging_avg_kudos,
  	p.avg_weekly_kudos AS avg_weekly_kudos,
  	p.std_weekly_kudos as std_weekly_kudos,
	--compute zscore for weekly kudos average
  	(staging_avg_kudos - avg_weekly_kudos) / std_weekly_kudos AS z_score
  FROM staging_table_avg_kudos s, kudos_by_week_statistics p
)

SELECT ABS(z_score) AS two_sized_zscore
FROM weekly_avg_kudos_zscore;