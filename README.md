# Serverless ETL Pipeline with AWS Glue, S3, and Athena

![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue)
![AWS SAM](https://img.shields.io/badge/IaC-AWS%20SAM-orange)
![Status](https://img.shields.io/badge/status-active-brightgreen)
![CI](https://github.com/IsahiRea/aws-etl-pipeline/actions/workflows/ci.yml/badge.svg)

A fully automated, serverless data pipeline that ingests raw data, transforms it using AWS Glue (PySpark), catalogs it, and enables SQL querying via Amazon Athena — triggered automatically by S3 file uploads via AWS Lambda.

---

## Architecture

```mermaid
flowchart LR
    A[CSV/JSON Upload] --> B[S3 Raw Bucket\nincoming/]
    B -->|S3 Event| C[Lambda\ntrigger_glue.py]
    C -->|StartJobRun| D[Glue ETL Job\netl_transform.py]
    D --> E[S3 Processed Bucket\ndata/state=XX/]
    E --> F[Glue Crawler]
    F --> G[Glue Data Catalog]
    G --> H[Amazon Athena]
    E --> H
    H --> I[QuickSight / Power BI]
```

---

## Tech Stack

| Service | Role |
|---|---|
| **Amazon S3** | Raw and processed data storage (data lake) |
| **AWS Glue** | PySpark ETL jobs + Data Catalog |
| **AWS Lambda** | Event-driven trigger on S3 upload |
| **Amazon Athena** | Serverless SQL querying on processed data |
| **AWS SAM** | Infrastructure-as-code deployment |
| **Python / PySpark** | ETL script language |

---

## Project Structure

```
aws-etl-pipeline/
├── .github/workflows/
│   └── ci.yml                  # GitHub Actions CI (pytest on push/PR)
├── glue_jobs/
│   └── etl_transform.py        # PySpark ETL: clean, validate, partition, write Parquet
├── lambda/
│   └── trigger_glue.py         # Lambda: trigger Glue on S3 upload
├── athena_queries/
│   ├── create_table.sql        # Partitioned external table DDL
│   └── sample_queries.sql      # Example analytical queries
├── tests/
│   ├── test_lambda_handler.py  # Lambda unit tests (mock Glue client)
│   └── test_etl_transform.py   # PySpark transform tests (local SparkSession)
├── data/raw/
│   └── sample.csv              # Synthetic sample data for testing
├── docs/
│   └── setup_guide.md          # Step-by-step AWS setup instructions
├── template.yaml               # SAM template (full IaC)
├── pytest.ini                  # Test configuration
├── requirements.txt
└── requirements-dev.txt
```

---

## Dataset

This project includes **synthetic sample data** (`data/raw/sample.csv`) for local testing. The schema mirrors the publicly available [CMS Medicare Provider Data](https://data.cms.gov/provider-summary-by-type-of-service):

| Column | Type | Description |
|---|---|---|
| provider_id | STRING | Unique provider identifier |
| provider_name | STRING | Hospital/facility name |
| **state** | **STRING** | **Two-letter state code (Athena partition key)** |
| total_discharges | INT | Number of patient discharges |
| avg_covered_charges | DOUBLE | Average charges billed |
| avg_total_payments | DOUBLE | Average total payments received |
| avg_medicare_payments | DOUBLE | Average Medicare payments |

To use real data, download from [data.gov](https://data.gov) and place CSVs in `data/raw/`.

---

## Deployment

### Option A: SAM (Recommended)

Requires [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html).

```bash
sam build
sam deploy --guided
```

This creates all resources: S3 buckets, Lambda function with S3 trigger, Glue job, Glue database, Glue crawler, and IAM roles.

### Option B: Manual CLI

See `docs/setup_guide.md` for step-by-step AWS CLI commands.

---

## Local Development

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

### Run Tests

```bash
# All tests
python -m pytest

# Lambda tests only
python -m pytest tests/test_lambda_handler.py -v

# ETL transform tests only (requires Java + PySpark)
python -m pytest tests/test_etl_transform.py -v
```

Lambda tests use `unittest.mock` to patch the Glue client — no AWS credentials needed. ETL tests use a local SparkSession (requires Java runtime).

---

## Pipeline Walkthrough

1. **Upload** a CSV file to `s3://<raw-bucket>/incoming/`
2. **Lambda** detects the upload event and triggers the Glue ETL job
3. **Glue** reads the raw CSV, validates the schema, cleans/transforms with PySpark
4. **Processed Parquet files** are written to `s3://<processed-bucket>/data/state=XX/` (partitioned by state)
5. **Athena** queries the processed data using the Glue Data Catalog
6. **QuickSight / Power BI** connects to Athena for visualization

---

## ETL Transform Details

The Glue job (`glue_jobs/etl_transform.py`) performs:

1. **Schema validation** — Fails fast if required columns are missing
2. **Null removal** — Drops rows where all values are null
3. **Column normalization** — Lowercase, spaces/hyphens to underscores
4. **Metadata enrichment** — Adds `_etl_processed_at` timestamp and `_etl_source` path
5. **Deduplication** — Removes exact duplicate rows
6. **Partitioned output** — Writes Snappy-compressed Parquet partitioned by `state`

---

## Cost Estimate

All services used are **AWS Free Tier eligible**:

| Service | Free Tier | When Costs Start |
|---|---|---|
| S3 | 5 GB storage, 20K GET, 2K PUT/month | Beyond storage/request limits |
| Lambda | 1M requests, 400K GB-seconds/month | Unlikely for this pipeline's volume |
| Glue | No free tier for ETL jobs | ~$0.44/DPU-hour (minimum 2 DPU, 10 min) |
| Athena | 5 TB scanned/month (first 12 months) | $5 per TB scanned after |

**Note:** Glue ETL jobs are the primary cost driver. For development, use the smallest worker configuration (2x G.1X in the SAM template).

---

## Skills Demonstrated

- Serverless architecture design on AWS (S3, Lambda, Glue, Athena)
- ETL pipeline development with PySpark
- Infrastructure-as-code with AWS SAM
- Data validation and schema enforcement
- Partitioned data lake storage patterns
- Unit testing with pytest
- CI/CD with GitHub Actions

---

## License

MIT
