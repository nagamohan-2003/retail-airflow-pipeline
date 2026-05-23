from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from datetime import datetime, timedelta


default_args = {
    "owner": "mohan",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}


with DAG(
    dag_id="retail_end_to_end_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=False,
    tags=["retail", "aws", "etl"]
) as dag:

    wait_for_raw_file = S3KeySensor(
        task_id="wait_for_raw_sales_file",
        bucket_name="retaildata-bucket",
        bucket_key="raw-sales/",
        aws_conn_id="aws_default",
        timeout=600,
        poke_interval=30
    )

    run_glue_job = GlueJobOperator(
        task_id="run_retail_glue_job",
        job_name="retail-sales-etl-job",
        aws_conn_id="aws_default",
        region_name="ap-southeast-2",
        wait_for_completion=True
    )

    validate_sales_data = AthenaOperator(
        task_id="validate_sales_data",
        query="""
            SELECT outlet_type,
                   SUM(item_outlet_sales) AS total_sales
            FROM curated_analytics
            GROUP BY outlet_type;
        """,
        database="retail_db",
        output_location="s3://retaildata-bucket/athena-results/",
        aws_conn_id="aws_default",
        region_name="ap-southeast-2",
        sleep_time=10,
        max_polling_attempts=60
    )

    wait_for_raw_file >> run_glue_job >> validate_sales_data