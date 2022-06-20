with activities_by_week AS (
  SELECT 
  	date_trunc('week', start_date::date) AS activity_week,
  	COUNT(*) AS activity_count           
  FROM public.strava_activity_data
  GROUP BY activity_week
  ORDER BY activity_week
),

activities_by_week_statistics AS (
  SELECT 
  	AVG(activity_count) AS avg_activities_per_week,
  	STDDEV(activity_count) AS std_activities_per_week
  FROM activities_by_week
),

staging_table_weekly_count AS (
  SELECT COUNT(*) AS staging_weekly_count
  FROM staging_table
),

activity_count_zscore AS (
  SELECT
  	s.staging_weekly_count AS staging_table_count,
  	p.avg_activities_per_week AS avg_activities_per_week,
  	p.std_activities_per_week as std_activities_per_week,
	--compute zscore for weekly activity count
  	(staging_table_count - avg_activities_per_week) / std_activities_per_week AS z_score
  FROM staging_table_weekly_count s, activities_by_week_statistics p
)

SELECT ABS(z_score) AS two_sized_zscore
FROM activity_count_zscore;