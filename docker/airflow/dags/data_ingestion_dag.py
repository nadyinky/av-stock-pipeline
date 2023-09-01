from datetime import datetime, date, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitSparkJobOperator

from data_pipeline import data_ingestion, data_processing
import data_pipeline.config as constants


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    # Setup error alerting:
    # 'email_on_failure': False,
    # 'email_on_retry': False,
}

dag = DAG(
    "data_ingestion_dag",
    default_args=default_args,
    description="Daily data ingestion DAG",
    schedule_interval="0 4 * * 1-5",  # Schedule to run every weekday at 4 AM UTC
    catchup=False,  # Skip any missed DAG runs
)

ingest_ticker_name_task = PythonOperator(
    task_id="ingest_ticker_name",
    python_callable=data_ingestion.ingest_ticker_name,
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)

check_gcs_file_task = GoogleCloudStoragePrefixSensor(
    task_id="check_gcs_file",
    bucket=constants.GCS_BUCKET_NAME,
    prefix=f"tickers/{date.today()}_tickers",
    dag=dag,
)

ingest_daily_ticker_info_task = PythonOperator(
    task_id="ingest_daily_ticker_info",
    python_callable=data_ingestion.ingest_daily_ticker_info,
    dag=dag,
)

ingest_gainers_losers_info_task = PythonOperator(
    task_id="ingest_gainers_losers_info",
    python_callable=data_ingestion.ingest_gainers_losers_info,
    dag=dag,
)

submit_spark_job_task = DataprocSubmitSparkJobOperator(
    task_id="submit_spark_job",
    cluster_name=constants.DPROC_CLUSTER_NAME,
    job_name="kafka_to_gcs",
    region=constants.DPROC_REGION,
    num_executors=2,
    executor_cores=2,
    executor_memory="2g",
    main_application="crs/kafka_connector/kafka_to_gcs.py",
    dag=dag,
)

ingest_ticker_name_task >> check_gcs_file_task >> [ingest_daily_ticker_info_task, ingest_gainers_losers_info_task] >> submit_spark_job_task
