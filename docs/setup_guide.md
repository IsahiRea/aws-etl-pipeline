# AWS Setup Guide

Step-by-step instructions to deploy the ETL pipeline on AWS.

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
5. Script path: `s3://your-etl-pipeline-raw/scripts/etl_transform.py`
6. Add job parameters:
   - `--raw_bucket` → `your-etl-pipeline-raw`
   - `--processed_bucket` → `your-etl-pipeline-processed`

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

## 7. Run the Glue Crawler

1. Go to **AWS Glue → Crawlers → Create Crawler**
2. Data source: `s3://your-etl-pipeline-processed/data/`
3. Database: `etl_pipeline_db`
4. Run the crawler to populate the Glue Data Catalog

---

## 8. Query with Athena

1. Go to **Amazon Athena → Query Editor**
2. Set output location: `s3://your-etl-pipeline-processed/athena-results/`
3. Run `athena_queries/create_table.sql`
4. Run queries from `athena_queries/sample_queries.sql`

---

## 9. Test the Full Pipeline

```bash
aws s3 cp data/raw/sample.csv s3://your-etl-pipeline-raw/incoming/sample.csv
```

Then check:
- Lambda logs in **CloudWatch**
- Glue job run status in **AWS Glue → Jobs**
- Processed files in `s3://your-etl-pipeline-processed/data/`
- Query results in **Athena**
