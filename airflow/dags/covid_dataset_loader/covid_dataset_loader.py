import json
import pendulum
import pandas as pd
from typing import Dict, NoReturn
from airflow import DAG
from sodapy import Socrata
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


DAG_NAME = 'covid_dataset_loader'
PATH_CONFIG = f'dags/{DAG_NAME}/configs/{DAG_NAME}.json'
PATH_SAVE = './data.cdc.gov.csv'
README = f'dags/{DAG_NAME}/README.md'
BUCKET_NAME = 'bigdata-winter2022'
DESTINATION_FILE_LOCATION = 'covid_raw/'


with open(PATH_CONFIG) as f:
    json_config = json.load(f)

with open(README, "r") as f:
    readme = f.read()


def extract_transform_load(**kwargs) -> NoReturn:
    """
    Unauthenticated client only works with public data sets.
    Note 'None' in place of app token, and no username or password.
    All results, returned as JSON from API.
    Converted to Python list of dictionaries by sodapy.
    """

    dataset_id: str = kwargs['dataset_id']
    client: Socrata = Socrata("data.cdc.gov", None)
    results: Dict = client.get_all(dataset_id)
    results_df: pd.DataFrame = pd.DataFrame.from_records(results)
    results_df.to_csv(PATH_SAVE)


with DAG(
        DAG_NAME,
        doc_md='README',
        default_args=json_config,
        schedule_interval=None,
        start_date=pendulum.datetime(2022, 2, 16),
        catchup=False,
) as dag:
    download_covid_dataset = PythonOperator(
        task_id='t1_get_request_from_api',
        python_callable=extract_transform_load,
        provide_context=True,
        dag=dag,
        op_kwargs={
            "dataset_id": json_config["arguments"]["dataset_id"]
        }
    )
    upload_file_to_gcp = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_gcp",
        src=PATH_SAVE,
        dst=DESTINATION_FILE_LOCATION,
        bucket=BUCKET_NAME,
        dag=dag
    )
    download_covid_dataset >> upload_file_to_gcp
