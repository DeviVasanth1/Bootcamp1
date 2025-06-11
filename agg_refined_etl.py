from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

spark = SparkSession.builder.appName("AggRefinedETL").getOrCreate()

# Load curated data
accounts_df = spark.read.parquet("gs://devi-project-bucket/curated/accounts/")
customers_df = spark.read.parquet("gs://devi-project-bucket/curated/customers/")

# Join on customer_id
joined_df = accounts_df.join(customers_df, on="customer_id", how="inner")

# Compute total account balance per customer
total_balance_df = accounts_df.groupBy("customer_id").agg(_sum("balance").alias("total_balance"))

# Join total balance back to the joined dataframe
result_df = joined_df.join(total_balance_df, on="customer_id", how="left")

# Select all columns from accounts and customers plus total_balance
# (joined_df already contains all columns from both)
# Optionally reorder columns if you want, here just select all
final_df = result_df

# Write output to Gold layer bucket in parquet format
final_df.write.mode("overwrite").parquet("gs://devi-project-bucket/agg-refined/")

spark.stop()
