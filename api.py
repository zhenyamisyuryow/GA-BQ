from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import csv
from google.oauth2 import service_account
import googleapiclient.discovery
import os
scopes = 'https://www.googleapis.com/auth/analytics.readonly'
credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scopes=scopes)
service = build('analytics', 'v3', credentials=credentials)
profiles = service.management().profiles().list(accountId='~all',webPropertyId='~all').execute()
views_info = {}
metrics = 'ga:users, ga:sessions, ga:goalCompletionsAll, ga:totalEvents'
dimensions = 'ga:date'
def get_data(view_id):
    view_data = service.data().ga().get(
        ids='ga:' + view_id,
        start_date = 'today',
        end_date = 'today',
        dimensions = dimensions,
        metrics = metrics
    ).execute()
    return view_data
for item in profiles.get('items', []):
    view_id = item.get('id')
    view_name = item.get('name')
    data = get_data(view_id)
    names = ['date', 'users','sessions','goalCompletionsAll']
    if 'rows' in data:
        arr = data.get('rows')[0]
        #views_info[view_id] = {'viewname':view_name, 'viewdata':get_data(view_id).get('rows')[0]}
    else:
        arr = ['0','0','0','0',]
    d = dict(zip(names, arr))
    views_info[view_id] = {'viewname':view_name, **d}
    date = views_info[view_id]['date']
    date = '-'.join((date[:4], date[4:6], date[6:]))
with open('data.csv', 'w', newline='') as outfile:
    writer = csv.writer(outfile)
    for key, value in views_info.items():
        writer.writerow([date, value['users'], value['sessions'], value['goalCompletionsAll'], key, value['viewname']])

from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client(location='US')

table_id = "test_GA_BigQ.python_test"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,  autodetect=True,
    schema=[
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("users", "INTEGER"),
        bigquery.SchemaField("sessions", "INTEGER"),
        bigquery.SchemaField("goal1Completions", "INTEGER"),
        bigquery.SchemaField("viewid", "INTEGER"),
        bigquery.SchemaField("viewname", "STRING"),
    ]
)

with open('data.csv', "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config, location='US')

job.result()  # Waits for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)