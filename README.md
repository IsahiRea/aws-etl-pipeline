# Serverless ETL Pipeline with AWS Glue, S3, and Athena

A fully automated, serverless data pipeline that ingests raw data, transforms it using AWS Glue, catalogs it, and enables SQL querying via Amazon Athena — triggered automatically by S3 file uploads via AWS Lambda.

---

## Architecture

```
Raw CSV/JSON → S3 (raw/) → Lambda Trigger → Glue ETL Job → S3 (processed/) → Athena → QuickSight / Power BI
                                                               ↓
                                                        Glue Data Catalog
```

---

## Tech Stack

| Service | Role |
|---|---|
| **Amazon S3** | Raw and processed data storage |
| **AWS Glue** | ETL jobs + Data Catalog |
| **AWS Lambda** | Trigger Glue job on S3 file upload |
| **Amazon Athena** | SQL querying on processed data |
| **Amazon QuickSight** | Data visualization (optional) |
| **Python / PySpark** | Glue ETL script language |

---

## Project Structure

```
aws-etl-pipeline/
├── data/
│   └── raw/                    # Sample input datasets (CSV)
├── glue_jobs/
│   └── etl_transform.py        # PySpark ETL script for AWS Glue
├── lambda/
│   └── trigger_glue.py         # Lambda function to trigger Glue on S3 upload
├── athena_queries/
│   ├── create_table.sql        # Create external table in Athena
│   └── sample_queries.sql      # Example analytical queries
├── docs/
│   └── setup_guide.md          # Step-by-step AWS setup instructions
├── requirements.txt
└── README.md
```

---

## Dataset

This project uses publicly available **CMS Medicare Provider Data** (or substitute any CSV from [data.gov](https://data.gov)):
- Download: https://data.cms.gov/provider-summary-by-type-of-service
- Place CSV files in `data/raw/` before uploading to S3

---

## Setup & Deployment

### Prerequisites
- AWS Account (free tier eligible)
- AWS CLI configured (`aws configure`)
- Python 3.9+
- IAM roles for Glue and Lambda (see `docs/setup_guide.md`)

### 1. Create S3 Buckets
```bash
aws s3 mb s3://your-etl-pipeline-raw
aws s3 mb s3://your-etl-pipeline-processed
```

### 2. Upload the Glue ETL Script
```bash
aws s3 cp glue_jobs/etl_transform.py s3://your-etl-pipeline-raw/scripts/
```

### 3. Deploy the Lambda Function
```bash
cd lambda
zip trigger_glue.zip trigger_glue.py
aws lambda create-function \
  --function-name TriggerGlueETL \
  --runtime python3.9 \
  --handler trigger_glue.lambda_handler \
  --zip-file fileb://trigger_glue.zip \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/LambdaGlueRole
```

### 4. Set S3 Trigger on Lambda
```bash
aws s3api put-bucket-notification-configuration \
  --bucket your-etl-pipeline-raw \
  --notification-configuration file://docs/s3_notification.json
```

### 5. Create Glue Crawler & Database
```bash
aws glue create-database --database-input '{"Name": "etl_pipeline_db"}'
```
Then run the crawler via the AWS Console or CLI to populate the Glue Data Catalog.

### 6. Query with Athena
Run the SQL in `athena_queries/create_table.sql`, then explore with `sample_queries.sql`.

---

## Pipeline Walkthrough

1. **Upload** a CSV file to `s3://your-etl-pipeline-raw/incoming/`
2. **Lambda** detects the upload and triggers the Glue ETL job
3. **Glue** reads the raw CSV, cleans/transforms the data using PySpark
4. **Processed Parquet files** are written to `s3://your-etl-pipeline-processed/`
5. **Athena** queries the processed data using the Glue Data Catalog
6. **QuickSight / Power BI** connects to Athena for visualization

---

## Skills Demonstrated

- Serverless architecture design on AWS
- ETL pipeline development with PySpark
- Data cataloging and schema management
- Infrastructure automation with AWS CLI
- SQL analytics with Amazon Athena

---

## License

MIT
