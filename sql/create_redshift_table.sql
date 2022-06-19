CREATE TABLE IF NOT EXISTS public.strava_activity_data (
    "id" VARCHAR NULL PRIMARY KEY,
    "name" VARCHAR NULL,
    "distance" DECIMAL NULL,
    "moving_time" DECIMAL NULL,
    "elapsed_time" DECIMAL NULL,
    "total_elevation_gain" DECIMAL NULL,
    "type" VARCHAR NULL,
    "workout_type" VARCHAR NULL,
    "location_country" VARCHAR NULL,
    "achievement_count" INTEGER NULL,
    "kudos_count" INTEGER NULL,
    "comment_count" INTEGER NULL,
    "athlete_count" INTEGER NULL,
    "average_speed" DECIMAL NULL,
    "max_speed" DECIMAL NULL,
    "average_cadence" DECIMAL NULL,
    "average_temp" DECIMAL NULL,
    "average_heartrate" DECIMAL NULL,
    "max_heartrate" INTEGER NULL,
    "suffer_score" INTEGER NULL,
    "start_date" TIMESTAMP NULL,
    "timezone" VARCHAR NULL,
    "lat" DECIMAL NULL,
    "lng" DECIMAL NULL);