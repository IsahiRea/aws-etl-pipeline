# AWS Setup Guide

Step-by-step instructions to deploy the ETL pipeline on AWS.

## Recommended: Deploy with SAM

The fastest way to deploy everything is with the [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html):

```bash
sam build
sam deploy --guided
```

This creates all resources automatically: S3 buckets, Lambda function with S3 trigger, Glue job, Glue database, Glue crawler, and IAM roles.

### SAM Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ProjectName` | `etl-pipeline` | Prefix for all resource names |
| `GlueScriptBucket` | *(empty — uses raw bucket)* | S3 bucket for the Glue ETL script. If empty, the script is read from the raw bucket. |
| `GlueScriptKey` | `scripts/etl_transform.py` | S3 key path to the Glue ETL script |

To use a separate bucket for the Glue script:
```bash
sam deploy --guided --parameter-overrides GlueScriptBucket=my-scripts-bucket
```

After deploying, skip to [Step 8: Query with Athena](#8-query-with-athena) and [Step 9: Test the Full Pipeline](#9-test-the-full-pipeline).

---

## Manual Setup

If you prefer to set up each resource individually, follow the steps below.

## 1. IAM Roles

### Glue Role (`GlueETLRole`)
Attach these managed policies:
- `AWSGlueServiceRole`
- `AmazonS3FullAccess` (or scope to your buckets)
- `CloudWatchLogsFullAccess`

### Lambda Role (`LambdaGlueRole`)
Attach these managed policies:
- `AWSLambdaBasicExecutionRole`
- Custom inline policy:
```json
{
  "Effect": "Allow",
  "Action": ["glue:StartJobRun"],
  "Resource": "arn:aws:glue:*:*:job/etl-pipeline-job"
}
```

---

## 2. S3 Buckets

```bash
aws s3 mb s3://your-etl-pipeline-raw    --region us-east-1
aws s3 mb s3://your-etl-pipeline-processed --region us-east-1
```

Create the folder structure:
```bash
aws s3api put-object --bucket your-etl-pipeline-raw --key incoming/
aws s3api put-object --bucket your-etl-pipeline-raw --key scripts/
```

---

## 3. Upload Glue Script

```bash
aws s3 cp glue_jobs/etl_transform.py s3://your-etl-pipeline-raw/scripts/
```

---

## 4. Create the Glue Job

Via AWS Console:
1. Go to **AWS Glue → Jobs → Create Job**
2. Name: `etl-pipeline-job`
3. IAM Role: `GlueETLRole`
4. Type: Spark
5. Glue version: `4.0`
6. Worker type: `G.1X` (2 workers minimum)
7. Script path: `s3://your-etl-pipeline-raw/scripts/etl_transform.py`
8. Add job parameters:
   - `--raw_bucket` → `your-etl-pipeline-raw`
   - `--processed_bucket` → `your-etl-pipeline-processed`
   - `--catalog_database` → `etl_pipeline_db`
   - `--catalog_table` → `processed_data`
   - `--job-bookmark-option` → `job-bookmark-enable`
   - `--enable-glue-datacatalog` → `true`

---

## 5. Deploy Lambda

```bash
cd lambda
zip trigger_glue.zip trigger_glue.py

aws lambda create-function \
  --function-name TriggerGlueETL \
  --runtime python3.9 \
  --handler trigger_glue.lambda_handler \
  --zip-file fileb://trigger_glue.zip \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/LambdaGlueRole \
  --environment Variables="{GLUE_JOB_NAME=etl-pipeline-job,RAW_BUCKET=your-etl-pipeline-raw,PROCESSED_BUCKET=your-etl-pipeline-processed}"
```

---

## 6. Add S3 Trigger to Lambda

Via AWS Console:
1. Go to your Lambda function → **Add Trigger**
2. Source: **S3**
3. Bucket: `your-etl-pipeline-raw`
4. Event type: `PUT`
5. Prefix: `incoming/`

---

## 7. Glue Data Catalog

The ETL job uses `getSink` with `enableUpdateCatalog=True`, which automatically creates and updates the `etl_pipeline_db.processed_data` table in the Glue Data Catalog after each run. No manual crawler run is needed.

A Glue Crawler is still configured in the SAM template as a fallback — you can run it manually if the catalog table needs to be re-synced.

---

## 8. Query with Athena

1. Go to **Amazon Athena → Query Editor**
2. Set output location: `s3://your-etl-pipeline-processed/athena-results/`
3. The table `etl_pipeline_db.processed_data` should already exist (auto-created by the Glue job). If not, run `athena_queries/create_table.sql`.
4. Run `MSCK REPAIR TABLE etl_pipeline_db.processed_data;` to discover partition directories (`state=XX/`)
5. Run queries from `athena_queries/sample_queries.sql`

---

## 9. Test the Full Pipeline

```bash
aws s3 cp data/raw/sample.csv s3://your-etl-pipeline-raw/incoming/sample.csv
```

Then check:
- Lambda logs in **CloudWatch**
- Glue job run status in **AWS Glue → Jobs**
- Processed files in `s3://your-etl-pipeline-processed/data/state=XX/` (partitioned by state)
- Query results in **Athena** (run `MSCK REPAIR TABLE` again if new partitions were added)
