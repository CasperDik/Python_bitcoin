import pandas as pd
from sys import getsizeof
from google.cloud import bigquery
from bq_helper import BigQueryHelper
# pip install --upgrade google-cloud-bigquery
# pip install -e git+https://github.com/SohierDane/BigQuery_Helper#egg=bq_helper

# set google credentials
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "apikey.json"

# get bitcoin date between 2 timestamps
bq_assist = BigQueryHelper('bigquery-public-data', 'crypto_bitcoin')

# query
QUERY_TEMPLATE = """
SELECT
    timestamp,
    inputs.input_pubkey_base58 AS input_key,
    outputs.output_pubkey_base58 AS output_key,
    outputs.output_satoshis as satoshis
FROM `bigquery-public-data.bitcoin_blockchain.transactions`
    JOIN UNNEST (inputs) AS inputs
    JOIN UNNEST (outputs) AS outputs
WHERE timestamp BETWEEN {0} AND {1}
    AND outputs.output_satoshis  >= {2}
    AND inputs.input_pubkey_base58 IS NOT NULL
    AND outputs.output_pubkey_base58 IS NOT NULL
GROUP BY timestamp, input_key, output_key, satoshis
"""

# set begin and end date for query
begin_date = int(pd.Timestamp(year=2019, month=11, day=1, hour = 10, second = 49, tz = 'US/Central').timestamp())
days = 1
end_date = begin_date + 60*60*24*days

# set minimum transaction amount
min_satoshi_per_transaction = 0

# format query
query = QUERY_TEMPLATE.format(begin_date, end_date, min_satoshi_per_transaction)
# run query
transactions = bq_assist.query_to_pandas(query)

# export to dataframe
transactions.to_csv("transactions_bitcoin.csv", index=False)

