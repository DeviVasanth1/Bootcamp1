from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
from pyspark.sql.functions import col, to_timestamp, to_date

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("RawToSilverETL") \
    .getOrCreate()

# --- Configuration ---
PROJECT_ID = "your-gcp-project-id"  # Replace with your GCP project ID
BUCKET_NAME = "your-project-bucket" # Replace with your GCS bucket name

# Raw (Bronze) GCS paths for the CSV files
RAW_ACCOUNTS_PATH = f"gs://devi-project-bucket/raw/accounts.csv"
RAW_CUSTOMERS_PATH = f"gs://devi-project-bucket/raw/customers.csv"
RAW_LOAN_PAYMENTS_PATH = f"gs://devi-project-bucket/raw/loan_payments.csv"
RAW_LOANS_PATH = f"gs://devi-project-bucket/raw/loans.csv"
RAW_TRANSACTIONS_PATH = f"gs://devi-project-bucket/raw/transactions.csv"

# Curated (Silver) GCS output paths for Parquet/Delta
SILVER_ACCOUNTS_PATH = f"gs://devi-project-bucket/curated/accounts"
SILVER_CUSTOMERS_PATH = f"gs://devi-project-bucket/curated/customers"
SILVER_LOAN_PAYMENTS_PATH = f"gs://devi-project-bucket/curated/loan_payments"
SILVER_LOANS_PATH = f"gs://devi-project-bucket/curated/loans"
SILVER_TRANSACTIONS_PATH = f"gs://devi-project-bucket/curated/transactions"

# --- Explicit Schema Definitions ---
# IMPORTANT: You MUST verify and adjust these schemas based on the actual column names,
# data types, and date/timestamp formats in your raw CSV files.
# Incorrect schemas will lead to parsing errors or incorrect data.

accounts_schema = StructType([
    StructField("account_id", StringType(), True), # Using StringType as IDs can be alphanumeric
    StructField("customer_id", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("account_type", StringType(), True),
    StructField("opened_date", StringType(), True) # Read as string initially, then cast
])

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("dob", StringType(), True) # Read as string initially, then cast
])

loan_payments_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("loan_id", StringType(), True),
    StructField("payment_date", StringType(), True), # Read as string initially, then cast
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

loans_schema = StructType([
    StructField("loan_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("loan_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("start_date", StringType(), True), # Read as string initially, then cast
    StructField("end_date", StringType(), True),   # Read as string initially, then cast
    StructField("status", StringType(), True)
])

transactions_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("transaction_date", StringType(), True), # Read as string initially, then cast
    StructField("amount", DoubleType(), True),
    StructField("type", StringType(), True),
    StructField("description", StringType(), True)
])

# --- Function to process each CSV file ---
def process_csv_to_silver(
    spark_session,
    raw_path,
    silver_path,
    schema,
    date_cols_to_convert=None, # List of (col_name, format_string, target_type)
    nullable_cols_to_drop_row=None, # List of column names where nulls mean drop the row
    file_name="Unknown"
):
    print(f"\n--- Processing {file_name} ---")
    print(f"Reading data from: {raw_path}")
    df = spark_session.read.format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(raw_path)

    print(f"Initial schema for {file_name}:")
    df.printSchema()
    df.show(5, truncate=False)

    # --- Data Cleaning ---
    # 1. Handle Nulls: Remove rows where critical columns are null
    if nullable_cols_to_drop_row:
        print(f"Dropping rows with nulls in critical columns: {nullable_cols_to_drop_row}")
        df = df.na.drop(subset=nullable_cols_to_drop_row)
        print(f"Row count after dropping nulls: {df.count()}")

    # 2. Convert Data Types (especially dates/timestamps)
    # The format_string is crucial and must match your CSV data's date/time format.
    if date_cols_to_convert:
        print(f"Converting date/timestamp columns: {date_cols_to_convert}")
        for col_name, format_string, target_type in date_cols_to_convert:
            if target_type == TimestampType:
                df = df.withColumn(col_name, to_timestamp(col(col_name), format_string))
            elif target_type == DateType:
                df = df.withColumn(col_name, to_date(col(col_name), format_string))
            else:
                print(f"Warning: Unsupported target type for date conversion: {target_type}")

    # You can add more specific cleaning rules here based on your data quality needs:
    # - Removing duplicates: `df = df.dropDuplicates(["primary_key_column"])`
    # - Filtering out malformed records: `df = df.filter(col("some_column").isNotNull())`
    # - Data validation: `df = df.filter(col("amount") > 0)`

    print(f"Schema after data cleaning and type conversion for {file_name}:")
    df.printSchema()
    df.show(5, truncate=False)

    # --- Write to Curated (Silver) ---
    print(f"Writing cleaned data to: {silver_path} in Parquet format...")
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(silver_path)

    # --- Note on Incremental/Delta Processing ---
    # For true incremental processing with Dataproc, especially with large datasets,
    # you would typically leverage a data lake format like Delta Lake.
    # If using Delta Lake:
    # 1. Ensure Delta Lake libraries are installed on your Dataproc cluster (e.g., via --jars).
    # 2. Change `.format("parquet")` to `.format("delta")`.
    # 3. For incremental updates (upserts/merges), you would use `delta.tables.DeltaTable`
    #    to perform a `merge` operation instead of a simple `overwrite`.
    #    Example (conceptual, requires `delta` import):
    #    from delta.tables import DeltaTable
    #    if DeltaTable.isDeltaTable(spark_session, silver_path):
    #        deltaTable = DeltaTable.forPath(spark_session, silver_path)
    #        deltaTable.alias("target").merge(
    #            source=df.alias("source"),
    #            condition="target.<your_primary_key> = source.<your_primary_key>"
    #        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    #    else:
    #        df.write.format("delta").mode("overwrite").save(silver_path)


    print(f"Finished processing process_data.py")


# --- Main Processing Calls for each file ---
# Call the function for each file with its specific schema and cleaning rules.
# Adjust the 'date_cols_to_convert' tuples (column_name, date_format_string, target_data_type)
# and 'nullable_cols_to_drop_row' lists based on your actual data and requirements.

process_csv_to_silver(
    spark,
    RAW_ACCOUNTS_PATH,
    SILVER_ACCOUNTS_PATH,
    accounts_schema,
    date_cols_to_convert=[("opened_date", "yyyy-MM-dd", DateType)],
    nullable_cols_to_drop_row=["account_id", "customer_id", "balance"],
    file_name="Accounts Data"
)

process_csv_to_silver(
    spark,
    RAW_CUSTOMERS_PATH,
    SILVER_CUSTOMERS_PATH,
    customers_schema,
    date_cols_to_convert=[("dob", "yyyy-MM-dd", DateType)],
    nullable_cols_to_drop_row=["customer_id", "email"],
    file_name="Customers Data"
)

process_csv_to_silver(
    spark,
    RAW_LOAN_PAYMENTS_PATH,
    SILVER_LOAN_PAYMENTS_PATH,
    loan_payments_schema,
    date_cols_to_convert=[("payment_date", "yyyy-MM-dd", DateType)],
    nullable_cols_to_drop_row=["payment_id", "loan_id", "amount"],
    file_name="Loan Payments Data"
)

process_csv_to_silver(
    spark,
    RAW_LOANS_PATH,
    SILVER_LOANS_PATH,
    loans_schema,
    date_cols_to_convert=[
        ("start_date", "yyyy-MM-dd", DateType),
        ("end_date", "yyyy-MM-dd", DateType)
    ],
    nullable_cols_to_drop_row=["loan_id", "customer_id", "amount"],
    file_name="Loans Data"
)

process_csv_to_silver(
    spark,
    RAW_TRANSACTIONS_PATH,
    SILVER_TRANSACTIONS_PATH,
    transactions_schema,
    date_cols_to_convert=[("transaction_date", "yyyy-MM-dd HH:mm:ss", TimestampType)],
    nullable_cols_to_drop_row=["transaction_id", "account_id", "amount"],
    file_name="Transactions Data"
)

# Stop SparkSession
spark.stop()