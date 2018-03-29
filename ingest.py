import pandas as pd
import requests, re
from google.cloud import storage, bigquery

# pull data
payload = {'limit': 0}
req = requests.get("https://api.coinmarketcap.com/v1/ticker/", params=payload)

# pandas dataframe
df = pd.read_json(req.text, orient="records")

# save to gcs
BUCKET = 'djr-caserta'
gcs_client = storage.Client()
bucket_ref = gcs_client.get_bucket(BUCKET)
blob = bucket_ref.blob('crypto_currencies.csv')
blob.upload_from_string(df.to_csv(index=False))
blob.make_public()

# make bq schema
field_type_mapping = {
    'float64': 'FLOAT',
    'int64': 'INTEGER',
    'object': 'STRING'
}

def field_name_formatter(x):
    return '_' + x if re.compile('^[0-9]').match(x) else x

def pd_bq_schema(df):
    schema = zip(df.columns, df.dtypes)
    return [bigquery.SchemaField(field_name_formatter(name), field_type_mapping[dtype.name])
            for name, dtype in schema]

schema = pd_bq_schema(df)

# import to bigquery
bq_client = bigquery.Client()

dataset_ref = bq_client.dataset('crypto_currencies')
job_config = bigquery.LoadJobConfig()
job_config.schema = schema
job_config.skip_leading_rows = 1
job_config.source_format = bigquery.job.SourceFormat.CSV
job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE

load_job = bq_client.load_table_from_uri(
    'gs://{}/crypto_currencies.csv'.format(BUCKET),
    dataset_ref.table('tickers'),
    job_config=job_config)

load_job.result()

assert load_job.state == 'DONE'
assert bq_client.get_table(dataset_ref.table('tickers')).num_rows > 0
