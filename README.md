# Bootcamp1

# 🛠️ GCP Dataproc PySpark Pipeline: CSV to Parquet

This project demonstrates a simple yet robust ETL pipeline using **Google Cloud Dataproc** and **PySpark** to process CSV files stored in **Google Cloud Storage (GCS)**. It performs schema-defined loading, data cleaning, type conversion, and writes the output to a curated GCS bucket in Parquet format.

---

## 📂 Project Structure

scripts/
├── process_data.py # Main PySpark ETL script
README.md # 


---

## ✅ Requirements

- Google Cloud Project
- GCS Bucket (e.g. `gs://devi-project-bucket`)
- Dataproc API enabled
- PySpark script uploaded to GCS
- Service account with access to Dataproc and GCS

---

## 🔁 ETL Flow Overview

1. **Input**: CSV files in `gs://devi-project-bucket/raw/`
2. **Transformation**:
   - Drop rows with null `id`, `name`, or `email`
   - Cast `age` to `Integer`
   - Cast `signup_date` to `Date`
3. **Output**: Parquet files written to `gs://devi-project-bucket/curated/`

---

## 🧱 Define the PySpark Script

Save the following as `process_data.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("GCS CSV Cleaning").getOrCreate()

schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", StringType(), True),
    StructField("signup_date", StringType(), True)
])

df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("gs://devi-project-bucket/raw/*.csv")

df_clean = df.dropna(subset=["id", "name", "email"])

df_transformed = df_clean \
    .withColumn("age", col("age").cast(IntegerType())) \
    .withColumn("signup_date", col("signup_date").cast(DateType()))

df_transformed.write.mode("overwrite").parquet("gs://devi-project-bucket/curated/")


