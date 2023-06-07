from airflow import DAG
import pendulum
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule


DAG_NAME = 'main_flow'


with DAG(
        dag_id=DAG_NAME,
        schedule_interval=None,
        start_date=pendulum.datetime(2022, 2, 16),
        catchup=False,
) as dag:
    trigger_covid_dataset_loader = TriggerDagRunOperator(
        task_id='trigger_covid_dataset_loader',
        trigger_dag_id='covid_dataset_loader',
        wait_for_completion=True
    )
    trigger_stocks_dataset_loader = TriggerDagRunOperator(
        task_id='trigger_stocks_dataset_loader',
        trigger_dag_id='stocks_dataset_loader',
        wait_for_completion=True
    )
    trigger_spark_job = TriggerDagRunOperator(
        task_id='trigger_spark_job',
        trigger_dag_id='spark_job',
        wait_for_completion=True
    )
    [trigger_covid_dataset_loader, trigger_stocks_dataset_loader] >> trigger_spark_job
