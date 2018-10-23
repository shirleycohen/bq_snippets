from google.cloud import bigquery
import datetime

src_project_id ='my_source_project'
src_dataset_id = 'my_source_dataset'  

dest_project_id = 'my_destination_project'
dest_dataset_id = 'my_destination_dataset'

table_name_prefix = 'events_' # set to table name prefix
start_date = datetime.date(2018, 06, 23) # set of first date to copy
end_date = datetime.date(2018, 10, 23) # set to last date to copy

config = bigquery.job.CopyJobConfig()
config.write_disposition = "WRITE_TRUNCATE"

client = bigquery.Client()
src_dataset = client.dataset(src_dataset_id, project=src_project_id)
dest_dataset = client.dataset(dest_dataset_id, project=dest_project_id)

flag = True
current_date = start_date

while flag == True:
    
    if current_date > end_date:
        flag = False
        # exit loop
    else:
        current_date_str = current_date.strftime("%Y%m%d")
        table_name = table_name_prefix + current_date_str
        
        src_table_ref = src_dataset.table(table_name)  
        dest_table_ref = dest_dataset.table(table_name)
    
        job = client.copy_table(src_table_ref, dest_table_ref, location='US', job_config=config)  
        job.result()  # Waits for job to complete.

        assert job.state == 'DONE'
        dest_table = client.get_table(dest_table_ref)  # API request
        assert dest_table.num_rows > 0
        print 'Info: DONE copying ' + table_name
        current_date = current_date + datetime.timedelta(days=1)
