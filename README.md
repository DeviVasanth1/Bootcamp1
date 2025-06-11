# Bootcamp1
# Customer Account Balance Daily Upsert Pipeline

This repository contains scripts and instructions to implement a daily upsert pipeline for the `customer_account_balance` table in Google BigQuery. The pipeline merges data from a staging table into a target table, updating existing records and inserting new ones.

---

## Overview

- **Target Table**: `customer_account_balance`
- **Staging Table**: dynamically created staging tables, e.g., `customer_account_balance_staging_YYYYMMDD_HHMMSS`
- **Operation**: Daily UPSERT (MERGE) from staging to target table
- **Tools Used**: Bash scripts, Google BigQuery, Cloud Shell, optionally Cloud Scheduler or Cloud Composer for scheduling

---

## Prerequisites

- Access to a Google Cloud Project with BigQuery API enabled
- Google Cloud SDK installed or access to Cloud Shell
- Appropriate permissions for BigQuery table creation and query execution

---

## Steps Performed

### 1. Prepare Target Table

- Create the target table `customer_account_balance` in BigQuery.
- Define the schema including columns such as `customer_id`, `first_name`, `last_name`, `account_id`, `balance`, `account_type`, `opened_date`, `email`, `phone`, `address`, `dob`, and `total_account_balance`.

### 2. Prepare Staging Table

- Create a staging table with the same schema as the target table.
- This staging table holds daily incremental or batch updates.

### 3. Create Daily Upsert Script (`daily_customer_balance_upsert.sh`)

- Write a bash script that runs a BigQuery MERGE SQL command.
- The MERGE command matches records on `customer_id`.
- Updates existing records with new data.
- Inserts new records if they don't exist in the target table.
- Use `CURRENT_TIMESTAMP()` or date functions as needed.

Example snippet inside the script:
4. Make Script Executable
chmod +x daily_customer_balance_upsert.sh
5. Run the Script Manually for Testing
./daily_customer_balance_upsert.sh

6. Validate the Upsert
Run SQL queries in BigQuery console to verify:

New records are inserted.

Existing records are updated with latest values.

Example query to check new customers not in the target table

SELECT DISTINCT S.customer_id 
FROM `project.dataset.customer_account_balance_staging` S
LEFT JOIN `project.dataset.customer_account_balance` T
ON S.customer_id = T.customer_id
WHERE T.customer_id IS NULL;
7. Schedule the Upsert Job
Use Cloud Scheduler with Cloud Functions or Cloud Composer (Airflow) to schedule this script daily.

Alternatively, run via cron job on a managed VM.
SELECT DISTINCT S.customer_id 
FROM `project.dataset.customer_account_balance_staging` S
LEFT JOIN `project.dataset.customer_account_balance` T
ON S.customer_id = T.customer_id
WHERE T.customer_id IS NULL;


Troubleshooting
Table Not Found Error: Verify the staging and target tables exist and are in the correct dataset and project.

Schema Mismatch: Ensure staging and target tables have matching schemas.

Permission Errors: Check that the service account or user running the script has BigQuery permissions.

Syntax Errors: Validate SQL syntax, especially in MERGE statements.

Additional Notes
Modify the script variables for your specific project, dataset, and table names.

Adjust the staging table name in the script dynamically if using multiple staging tables.

Test with sample data before running in production.

```bash
bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" <<EOF
MERGE \`${PROJECT_ID}.${DATASET}.${TARGET_TABLE}\` T
USING \`${PROJECT_ID}.${DATASET}.${STAGING_TABLE}\` S
ON T.customer_id = S.customer_id
WHEN MATCHED THEN
  UPDATE SET
    first_name = S.first_name,
    last_name = S.last_name,
    account_id = S.account_id,
    balance = S.balance,
    account_type = S.account_type,
    opened_date = S.opened_date,
    email = S.email,
    phone = S.phone,
    address = S.address,
    dob = S.dob,
    total_account_balance = S.total_account_balance
WHEN NOT MATCHED THEN
  INSERT (customer_id, first_name, last_name, account_id, balance, account_type, opened_date, email, phone, address, dob, total_account_balance)
  VALUES (S.customer_id, S.first_name, S.last_name, S.account_id, S.balance, S.account_type, S.opened_date, S.email, S.phone, S.address, S.dob, S.total_account_balance);
EOF
....



