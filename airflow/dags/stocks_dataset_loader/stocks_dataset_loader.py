import json
import pandas as pd
import pendulum
import yfinance as yf
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import time

DAG_NAME = 'stocks_dataset_loader'
PATH_CONFIG = f'dags/{DAG_NAME}/configs/{DAG_NAME}.json'
PATH_TO_HISTORY_DATASET = 'dataset/history'
PATH_SAVE = PATH_TO_HISTORY_DATASET + '/{}-{}.csv'
README = f'dags/{DAG_NAME}/README.md'

BUCKET_NAME = 'bigdata-winter2022'
DESTINATION_FILE_LOCATION = 'stocks_raw/'
PATH_LOCAL_SAVE = f'{PATH_TO_HISTORY_DATASET}/*.csv'

with open(PATH_CONFIG) as f:
    json_config = json.load(f)

with open(README, "r") as f:
    readme = f.read()

start = time.time()


def fetch_and_save_stocks_data(**kwargs):
    data = pd.read_csv(kwargs['stocks_symbols_api_url'], sep='|')  # download stock symbols
    data_clean = data[data['Test Issue'] == 'N']
    ticker_list = data_clean['NASDAQ Symbol'].tolist()
    print('total number of symbols traded = {}'.format(len(ticker_list)))

    def download_chunk_of_stocks(t_list):
        data_stocks = yf.download(
            tickers=t_list,
            period=kwargs['period'],
            interval=kwargs['interval'],
            group_by='ticker',
            auto_adjust=False,
            prepost=False,
            threads=True,
            proxy=None
        )

        d = data_stocks.stack(level=0).reset_index().rename(columns={'level_1': 'Ticker'}).set_index(
            'Ticker').sort_values(['Ticker', 'Date'])
        d.to_csv(PATH_SAVE.format(t_list[0], t_list[-1]))

    i = 0
    incr = 3000
    while i < len(ticker_list):
        download_chunk_of_stocks(ticker_list[i:i + incr])
        i += incr


print('It took', time.time() - start, 'seconds.')

with DAG(
        DAG_NAME,
        doc_md='README',
        default_args=json_config,
        schedule_interval=None,
        start_date=pendulum.datetime(2022, 2, 7),
        catchup=False
) as dag:
    fetch_and_save_stocks_data = PythonOperator(
        task_id='fetch_and_save_stocks_data',
        python_callable=fetch_and_save_stocks_data,
        dag=dag,
        op_kwargs={
            "stocks_symbols_api_url": json_config["arguments"]["stocks_symbols_api_url"],
            "period": json_config["arguments"]["period"],
            "interval": json_config["arguments"]["interval"]
        }
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_gcp",
        src=PATH_LOCAL_SAVE,
        dst=DESTINATION_FILE_LOCATION,
        bucket=BUCKET_NAME,
        dag=dag
    )
    fetch_and_save_stocks_data >> upload_file
