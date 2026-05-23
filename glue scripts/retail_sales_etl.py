import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
df = spark.table("retail_db.raw_sales")
from pyspark.sql.functions import col

df_clean = df.filter(
    col("item_outlet_sales").isNotNull() &
    (col("item_outlet_sales") > 0)
)

df_clean.show(5)

df_clean.write \
    .mode("overwrite") \
    .partitionBy("outlet_type") \
    .parquet("s3://retaildata-bucket/processed-sales/")
    
df_processed = spark.read.parquet(
    "s3://retaildata-bucket/processed-sales/"
)

from pyspark.sql.functions import sum

sales_by_outlet = df_processed.groupBy("outlet_type") \
    .agg(
        sum("item_outlet_sales").alias("total_sales")
    )

sales_by_outlet.show()

sales_by_outlet.write \
    .mode("overwrite") \
    .parquet("s3://retaildata-bucket/curated-analytics/sales_by_outlet/")
    
    
job.commit()