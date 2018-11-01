# Cloud Function that loads a CSV file as a BigQuery table
# Memory allocated: 1GB
# Trigger: Cloud Storage
# Event Type: Finalize/Create
# Bucket: <your bucket>
# Runtime: Python 3.7
# Timeout: 120 seconds 
# Function to execute: load_data
#
# requirements.txt must have the line:
# google-cloud-bigquery

from google.cloud import bigquery
import logging

default_dataset = 'your dataset' # set this to your default BQ dataset 

def extract_dataset(file_name):
    file_name_splits = file_name.split('_')
    num_splits = len(file_name_splits)
    if num_splits > 1:
        logging.info('file_name has ' + str(num_splits) + ' splits')
        dataset_name = file_name_splits[0].lower()
    else:
        dataset_name = default_dataset.lower()
    return dataset_name

def extract_table(file_name):
    file_name_without_extension = file_name.rsplit('.', 1)[0]
    logging.info('file_name_without_extension: ' + file_name_without_extension)
    
    file_name_splits = file_name_without_extension.split('_', 1)
    num_splits = len(file_name_splits)
    
    if num_splits > 1:
        table_name = file_name_splits[1]
    else:
        table_name = file_name_without_extension
        
    return table_name

def load_data(data, context):
    
    client = bigquery.Client()

    # GCS variables
    bucket = data['bucket']
    file_name = data['name']

    # BQ variables
    dataset_name = extract_dataset(file_name)
    logging.info('dataset_name: ' + dataset_name)
    
    table_name = extract_table(file_name)
    logging.info('table name: ' + table_name)

    dataset_ref = client.dataset(dataset_name)
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
