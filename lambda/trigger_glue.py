"""
AWS Lambda Function: trigger_glue.py
--------------------------------------
Triggered by an S3 PutObject event.
Starts the AWS Glue ETL job whenever a new file lands in the raw bucket.

IAM Requirements:
  - glue:StartJobRun
  - s3:GetObject on the raw bucket
"""

import json
import boto3
import os
import urllib.parse

glue_client = boto3.client("glue")

# Set these as Lambda environment variables in the AWS Console
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "etl-pipeline-job")
RAW_BUCKET = os.environ.get("RAW_BUCKET", "your-etl-pipeline-raw")
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "your-etl-pipeline-processed")


def lambda_handler(event, context):
    """
    Entry point for the Lambda function.
    Parses the S3 event and triggers the Glue ETL job.
    """
    print(f"[Lambda] Received event: {json.dumps(event)}")

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        print(f"[Lambda] New file detected: s3://{bucket}/{key}")

        # Only trigger on files in the /incoming/ prefix
        if not key.startswith("incoming/"):
            print(f"[Lambda] Skipping non-incoming file: {key}")
            continue

        # Only process CSV and JSON files
        if not (key.endswith(".csv") or key.endswith(".json")):
            print(f"[Lambda] Skipping unsupported file type: {key}")
            continue

        try:
            response = glue_client.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments={
                    "--raw_bucket": RAW_BUCKET,
                    "--processed_bucket": PROCESSED_BUCKET,
                    "--triggered_by_key": key,
                },
            )
            job_run_id = response["JobRunId"]
            print(f"[Lambda] Started Glue job run: {job_run_id}")

        except Exception as e:
            print(f"[Lambda] ERROR starting Glue job: {str(e)}")
            raise e

    return {"statusCode": 200, "body": "Glue job triggered successfully."}
