from airflow import DAG
import pendulum
import json
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator,\
    DataprocSubmitPySparkJobOperator, DataprocDeleteClusterOperator
from airflow.utils import trigger_rule


PATH_CONFIG = f'dags/spark_job_in_airflow_dag/configs/config_spark_job.json'
PYSPARK_URI = 'gs://bigdata-winter2022/Spark_job_example/spark_job_example.py'
CLUSTER_NAME = 'bigdata-cluster-{{ ts_nodash | lower }}'


with open(PATH_CONFIG, "r") as jsonfile:
    json_config = json.load(jsonfile)


GCP_REGION = json_config["GCP_CONFIG"]["GCP_REGION"]
GCP_PROJECT = json_config["GCP_CONFIG"]["GCP_PROJECT"]
GCP_BUCKET = json_config["GCP_CONFIG"]["GCP_BUCKET"]


default_dag_args = {
    'start_date': pendulum.datetime(2022, 2, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'location': GCP_REGION,
    'project_id': GCP_PROJECT
}


with DAG(
        json_config["dag"]["DAG_NAME"],
        default_args=default_dag_args,
        schedule_interval=None
) as dag:
    create_dataproc_cluster = DataprocCreateClusterOperator(
        project_id=GCP_PROJECT,
        task_id='create_dataproc_cluster',
        cluster_name=CLUSTER_NAME,
        cluster_config=json_config["CLUSTER_CONFIG"],
        region=GCP_REGION,
        labels={'team': 'bigdata'}
    )
    run_dataproc_spark = DataprocSubmitPySparkJobOperator(
        task_id='run_dataproc_spark',
        main=PYSPARK_URI,
        project_id=GCP_PROJECT,
        cluster_name=CLUSTER_NAME,
        region=GCP_REGION
    )
    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        project_id=GCP_PROJECT,
        task_id='delete_dataproc_cluster',
        cluster_name=CLUSTER_NAME,
        region=GCP_REGION,
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE
    )
    create_dataproc_cluster >> run_dataproc_spark >> delete_dataproc_cluster
