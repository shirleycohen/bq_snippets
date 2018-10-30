# Cloud Function that loads a CSV file as a BigQuery table
# 
# requirements.txt should contain the line:
# google-cloud-bigquery

from google.cloud import bigquery
import logging

def load_data(data, context):
    
    client = bigquery.Client()

    # GCS variables
    bucket = data['bucket']
    file_name = data['name']

    # BQ variables
    dataset_id = 'your_dataset'
    table_name = file_name.rsplit('.', 1)[0]
    logging.info('table name: ' + table_name)

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)  

    config = bigquery.job.LoadJobConfig()
    config.allow_jagged_rows = True
    config.allow_quoted_newlines = True
    config.create_disposition = 'CREATE_IF_NEEDED'
    config.field_delimiter = ','
    config.autodetect = True
    config.source_format = 'CSV'
    config.write_disposition = 'WRITE_TRUNCATE'
    config.skip_leading_row = 1

    uri ='gs://' + bucket + '/' + file_name
    logging.info('uri: ' + uri)

    load_job = job = client.load_table_from_uri(uri, table_ref, job_config=config)

    logging.info('Starting job {}'.format(load_job.job_id))
    load_job.result()  # Waits for table load to complete.
    logging.info('Job finished.')
    destination_table = client.get_table(table_ref)
    logging.info('Loaded {} rows.'.format(destination_table.num_rows))
