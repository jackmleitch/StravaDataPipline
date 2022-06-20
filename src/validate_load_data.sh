#!/bin/sh
python src/validator.py sql/validation/activity_dup.sql sql/validation/activity_dup_zero.sql equals warn
python src/validator.py sql/validation/weekly_activity_count_zscore.sql sql/validation/zscore_90_twosided.sql.sql greater_equals warn
python src/validator.py sql/validation/weekly_kudos_avg_zscore.sql sql/validation/zscore_90_twosided.sql.sql greater_equals warn
