import os
import datetime
import pytz
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

'''
Write results to Bigquery table
- First write dataframes to csv
- Second, upload csv to Cloud Storage.
- Third, load table from uri (Cloud Storage).
'''

def upload_to_bigquery(dataframes):
    '''Write files to csv'''
    for dataframe in dataframes:
        dataframe.to_csv("{}.csv".format(dataframe.name), index = False)

    '''Authenticate a service account'''
    key_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    storage_client = storage.Client.from_service_account_json(key_path)
    buckets = list(storage_client.list_buckets())
    
    '''
    Upload files to Cloud Storage
    https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    And build the file uris in Cloud Storage.
    '''
    file_uris = []
    for dataframe in dataframes:
        destination_blob_name = "{}.csv".format(dataframe.name)
        source_file_name = destination_blob_name
        bucket = storage_client.bucket(buckets[0].name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        '''print(  
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        ))'''
        file_uris.append("gs://{}/{}".format(buckets[0].name, destination_blob_name))
    '''
    Load data to Bigquery from Cloud Storage URIs.
    '''
    '''Construct the BigQuery client object used in this project.'''
    key_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    '''Build a list of table ids.'''
    table_ids = []
    for dataframe in dataframes:
        tbl_ref = client.dataset(os.environ.get('GOOGLE_BIGQUERY_DATASET')).table(dataframe.name)
        table_ids.append(tbl_ref) 

    '''Bigquery settings for load jobs.'''
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1)

    '''Load data from Cloud Storage to Bigquery.'''
    table_ids = table_ids
    for uri, id in zip(file_uris, table_ids):
        print("{} {}".format(uri, id))
        job = client.load_table_from_uri(uri, id, job_config=job_config)
        job.result()

    '''End of function.'''

'''
Query data from Bigquery
'''
def query_data(today):
    '''
    today date is not currently used.
    it should be used in prod.
    data from the day before should only be queried. 
    '''
    date = today - timedelta(1)
    date = datetime.strftime(date, '%Y-%m-%d')
    '''print(date)'''

    '''Authenticate a service account'''
    key_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    '''Construct the BigQuery client object used in this project.'''
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    '''Build the query.'''
    raw_data_table_id = os.environ.get('GOOGLE_BIGQUERY_PATH_RAW_DATA')
    query_string = """
        SELECT
        *
        FROM
        {}
    """.format(raw_data_table_id)

    dataframe = (
        client.query(query_string)
        .result()
        .to_dataframe(create_bqstorage_client=True)
        )

    return dataframe


'''
drops column based on column name
'''
def prestamo(data):
    data = data[data['event_name'].str.contains('prestamo_exito')]
    dataframe_prestamo = data.drop('event_name', axis = 1, inplace = True)
    
    return data

'''
Add date and timestamp to attribution results.
'''
def add_date(data):
    '''
    Get date and timestamp
    To build date and processing_time columns
    '''
    current_date = datetime.now().date().strftime("%Y-%m-%d")
    exact_time = datetime.now()
    timestamp = exact_time.timestamp()
    
    '''
    Add values to columns
    '''
    data['date'] = current_date
    data['processing_time'] = timestamp

    '''
    Return data as list
    '''
    return data


'''
Split channel_name into 'path' and 'platform' columns
only useful when column name to split is "channel_name"
'''
def channel_name_split(data):
    new_cols = data['channel_name'].str.split(' - ', n = 1, expand = True)
    data['path'] = new_cols[0]
    data['platform'] = new_cols[1]

    return data

def steps(data):
    '''
    Split "path" column and count the sessions.
    - First: get the conversion paths
    - Second: count the paths
    '''
    paths = data.loc[:,'path']
    conversions = data.loc[:,'conversiones']

    '''Count paths '''
    count = []
    for path in paths:
        count.append(path.count('>'))

    '''Split sessions in lists '''
    last_path = []
    for path in paths:
        last_path.append(
            path.split(' > ')
        )

    '''Loop list of lists and get last item for each list. '''
    last_channel = [item[-1] for item in last_path]

    '''Merge lists into a dataframe.'''
    last_touch = pd.DataFrame(list(zip(last_channel, count, conversions)),
                columns=['channel_name','session_count', 'conversions'])
    
    '''Loop list of lists and get first item for each list. '''
    first_channel = [item[0] for item in last_path]

    '''Merge lists into a dataframe.'''
    first_touch = pd.DataFrame(list(zip(first_channel, count, conversions)),
                columns=['channel_name','session_count', 'conversions'])

    return first_touch, last_touch