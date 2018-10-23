from google.cloud import bigquery
import datetime

src_project_id ='my_source_project'
src_dataset_id = 'my_source_dataset'  

dest_project_id = 'my_destination_project'
dest_dataset_id = 'my_destination_dataset'

table_name_prefix = 'events_'
todays_date = datetime.datetime.today()
yesterdays_date = todays_date - datetime.timedelta(1)

client = bigquery.Client()
src_dataset = client.dataset(src_dataset_id, project=src_project_id)
dest_dataset = client.dataset(dest_dataset_id, project=dest_project_id)

todays_date_str = todays_date.strftime("%Y%m%d")
todays_table_name = table_name_prefix + todays_date_str
print "INFO: Today's table: " + todays_table_name

config = bigquery.job.CopyJobConfig()
config.write_disposition = "WRITE_TRUNCATE"

iterator = client.list_tables(dest_dataset)

table_exists = False

for it in iterator:
    table_id = it.table_id
    if table_id == todays_table_name:
        table_exists = True
        break

# today's table doesn't exist yet, make final copy of yesterday's table before proceeding
if table_exists == False:
    print "INFO: Today's table doesn't exist yet in project " +  dest_project_id + ". Making final copy of yesterday's table before copying today's table. "
    yesterdays_date_str = yesterdays_date.strftime("%Y%m%d") 
    yesterdays_table_name = table_name_prefix + yesterdays_date_str
    print "INFO: Yesterday's table: " + yesterdays_table_name
    
    src_table_ref = src_dataset.table(yesterdays_table_name)  
    dest_table_ref = dest_dataset.table(yesterdays_table_name)
    
    job = client.copy_table(src_table_ref, dest_table_ref, location='US', job_config=config)  
    job.result()  # Waits for job to complete.

    assert job.state == 'DONE'
    dest_table = client.get_table(dest_table_ref)  # API request
    assert dest_table.num_rows > 0
    print 'Info: DONE copying ' + yesterdays_table_name
    

# copy today's table
src_table_ref = src_dataset.table(todays_table_name)  
dest_table_ref = dest_dataset.table(todays_table_name)
    
job = client.copy_table(src_table_ref, dest_table_ref, location='US', job_config=config)  
job.result()  # Waits for job to complete.

assert job.state == 'DONE'
dest_table = client.get_table(dest_table_ref)  # API request
assert dest_table.num_rows > 0
print 'Info: DONE copying ' + todays_table_name

