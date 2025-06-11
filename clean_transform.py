from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

spark = SparkSession.builder.appName("CleanTransform").getOrCreate()

# Define schemas for all 5 CSVs

accounts_schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True)
])

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True)
])

loan_payments_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("loan_id", StringType(), True),
    StructField("payment_date", TimestampType(), True),
    StructField("payment_amount", DoubleType(), True)
])

loans_schema = StructType([
    StructField("loan_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("loan_amount", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("loan_term", IntegerType(), True)
])

transactions_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True)
])

# Read and clean each CSV

accounts_df = spark.read.schema(accounts_schema).option("header", True).csv("gs://devi-project-bucket/raw/accounts.csv")
customers_df = spark.read.schema(customers_schema).option("header", True).csv("gs://devi-project-bucket/raw/customers.csv")
loan_payments_df = spark.read.schema(loan_payments_schema).option("header", True).csv("gs://devi-project-bucket/raw/loan_payments.csv")
loans_df = spark.read.schema(loans_schema).option("header", True).csv("gs://devi-project-bucket/raw/loans.csv")
transactions_df = spark.read.schema(transactions_schema).option("header", True).csv("gs://devi-project-bucket/raw/transactions.csv")

# Example cleaning: drop rows with null keys in each dataframe
accounts_clean = accounts_df.dropna(subset=["account_id"])
customers_clean = customers_df.dropna(subset=["customer_id"])
loan_payments_clean = loan_payments_df.dropna(subset=["payment_id"])
loans_clean = loans_df.dropna(subset=["loan_id"])
transactions_clean = transactions_df.dropna(subset=["transaction_id"])

# Write cleaned data to curated bucket (overwrite mode)
accounts_clean.write.mode("overwrite").parquet("gs://devi-project-bucket/curated/accounts/")
customers_clean.write.mode("overwrite").parquet("gs://devi-project-bucket/curated/customers/")
loan_payments_clean.write.mode("overwrite").parquet("gs://devi-project-bucket/curated/loan_payments/")
loans_clean.write.mode("overwrite").parquet("gs://devi-project-bucket/curated/loans/")
transactions_clean.write.mode("overwrite").parquet("gs://devi-project-bucket/curated/transactions/")
