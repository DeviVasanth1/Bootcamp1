from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, current_timestamp

# Create Spark session
spark = SparkSession.builder.appName("CustomerAccountETL").getOrCreate()

# Load curated customer and account data
customers_df = spark.read.parquet("gs://devi-project-bucket/curated/customers/")
accounts_df = spark.read.parquet("gs://devi-project-bucket/curated/accounts/")

# Join on customer_id
joined_df = customers_df.join(accounts_df, on="customer_id", how="inner")

# Compute total balance per customer
agg_df = joined_df.groupBy("customer_id").agg(
    spark_sum("balance").alias("balance")
)

# Add last_updated timestamp
result_df = joined_df.join(agg_df, on="customer_id", how="inner") \
    .withColumn("last_updated", current_timestamp())
    result_df.printSchema()
result_df = result_df.dropDuplicates(["customer_id", "account_id"])

# Write final result to Gold layer
result_df.write.mode("overwrite").parquet("gs://devi-project-bucket/agg-refined/customer_account_balances/")
result_df.printSchema()

