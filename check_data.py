from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count

spark = SparkSession.builder.appName("CheckCleanedData").getOrCreate()

# Load the parquet file (change path accordingly)
df = spark.read.parquet("gs://devi-project-bucket/curated/accounts/")

# Show schema and few rows
df.printSchema()
df.show(5)

# Count total rows
total_rows = df.count()
print(f"Total rows: {total_rows}")

# Count rows with nulls in key columns (e.g. account_id)
null_count = df.filter(col("account_id").isNull()).count()
print(f"Rows with null account_id: {null_count}")

# Count all nulls per column
nulls_per_column = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
nulls_per_column.show()
