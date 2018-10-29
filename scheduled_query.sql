# Example that copies yesterday's firebase table to another table in BQ via a Scheduled Query in BigQuery
# 
# Scheduled Query parameters:
# Destination: <dataset_destination>
# Query string: see below
# Destination table: events_{run_time-24h|"%Y%m%d"}
# Write preference: WRITE_TRUNCATE
# Partitioning field: N/A
# Next import run: N/A
# Schedule: every day at 15:05
# Email notifications: On transfer failures

# Query string:
select * 
from `scohen-firebase-sandbox.analytics_153293282.events_*`
where _TABLE_SUFFIX = replace(cast(DATE_SUB(@run_date, INTERVAL 1 DAY) as STRING), '-', '')

# Additional notes: 
# before scheduling query, display destination project as the default project in BigQuery legacy UI
# after scheduling query, use manual runs option to backfill previous dates (e.g. 2018-10-20 - 2018-10-24)
