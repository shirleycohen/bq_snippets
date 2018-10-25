# Example that copies the current firebase table each day via a Scheduled Query in BigQuery
# 
# Scheduled Query parameters:
# Destination: <dataset_destination>
# Query string: select * from `<source project>.<source dataset>.<table prefix>_*` where _TABLE_SUFFIX = replace(cast(@run_date as STRING), '-', '')
# Destination table: events_{run_date}
# Write preference: WRITE_TRUNCATE
# Partitioning field: N/A
# Next import run: N/A
# Schedule: every day at 15:05
# Email notifications: On transfer failures

# Example query string:
select * 
from `scohen-firebase-sandbox.analytics_153293282.events_*`
where _TABLE_SUFFIX = replace(cast(@run_date as STRING), '-', '')

# Additional notes: 
# before scheduling query, display destination project as the default project in BigQuery legacy UI
# after scheduling query, use manual runs option to backfill previous dates (e.g. 2018-10-20 - 2018-10-24)
