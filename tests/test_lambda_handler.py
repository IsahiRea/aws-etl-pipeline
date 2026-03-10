"""Tests for Lambda trigger_glue handler."""

import json
import os
import importlib
from unittest.mock import patch, MagicMock

import pytest

# Set env vars before importing the module
os.environ["GLUE_JOB_NAME"] = "test-etl-job"
os.environ["RAW_BUCKET"] = "test-raw-bucket"
os.environ["PROCESSED_BUCKET"] = "test-processed-bucket"


def _make_s3_event(bucket, key):
    """Create a mock S3 PutObject event."""
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key},
                }
            }
        ]
    }


# We need to handle that 'lambda' is a Python keyword.
# Import the module using importlib.
@pytest.fixture(autouse=True)
def _patch_boto(monkeypatch):
    """Patch boto3.client before importing the lambda module."""
    mock_glue = MagicMock()
    mock_glue.start_job_run.return_value = {"JobRunId": "jr_test_123"}

    original_client = __import__("boto3").client

    def mock_client(service, **kwargs):
        if service == "glue":
            return mock_glue
        return original_client(service, **kwargs)

    monkeypatch.setattr("boto3.client", mock_client)

    # Force reimport to pick up the mocked client
    import importlib
    mod = importlib.import_module("lambda.trigger_glue")
    importlib.reload(mod)

    # Store references for tests
    _patch_boto.mock_glue = mock_glue
    _patch_boto.handler = mod.lambda_handler


def test_triggers_glue_on_csv_upload():
    mock_glue = _patch_boto.mock_glue
    handler = _patch_boto.handler
    mock_glue.start_job_run.reset_mock()

    event = _make_s3_event("test-raw-bucket", "incoming/data.csv")
    result = handler(event, None)

    mock_glue.start_job_run.assert_called_once()
    call_args = mock_glue.start_job_run.call_args
    assert call_args.kwargs["JobName"] == "test-etl-job"
    assert call_args.kwargs["Arguments"]["--raw_bucket"] == "test-raw-bucket"
    assert result["statusCode"] == 200


def test_triggers_glue_on_json_upload():
    mock_glue = _patch_boto.mock_glue
    handler = _patch_boto.handler
    mock_glue.start_job_run.reset_mock()

    event = _make_s3_event("test-raw-bucket", "incoming/data.json")
    result = handler(event, None)

    mock_glue.start_job_run.assert_called_once()
    assert result["statusCode"] == 200


def test_skips_non_incoming_prefix():
    mock_glue = _patch_boto.mock_glue
    handler = _patch_boto.handler
    mock_glue.start_job_run.reset_mock()

    event = _make_s3_event("test-raw-bucket", "archive/data.csv")
    result = handler(event, None)

    mock_glue.start_job_run.assert_not_called()
    assert result["statusCode"] == 200


def test_skips_unsupported_file_type():
    mock_glue = _patch_boto.mock_glue
    handler = _patch_boto.handler
    mock_glue.start_job_run.reset_mock()

    event = _make_s3_event("test-raw-bucket", "incoming/image.png")
    result = handler(event, None)

    mock_glue.start_job_run.assert_not_called()
    assert result["statusCode"] == 200


def test_handles_url_encoded_keys():
    mock_glue = _patch_boto.mock_glue
    handler = _patch_boto.handler
    mock_glue.start_job_run.reset_mock()

    event = _make_s3_event("test-raw-bucket", "incoming/my+file+name.csv")
    result = handler(event, None)

    mock_glue.start_job_run.assert_called_once()
    assert result["statusCode"] == 200


def test_raises_on_glue_error():
    mock_glue = _patch_boto.mock_glue
    handler = _patch_boto.handler
    mock_glue.start_job_run.reset_mock()
    mock_glue.start_job_run.side_effect = Exception("Glue unavailable")

    event = _make_s3_event("test-raw-bucket", "incoming/data.csv")

    with pytest.raises(Exception, match="Glue unavailable"):
        handler(event, None)

    # Reset side_effect for other tests
    mock_glue.start_job_run.side_effect = None
    mock_glue.start_job_run.return_value = {"JobRunId": "jr_test_123"}


def test_handles_empty_records():
    mock_glue = _patch_boto.mock_glue
    handler = _patch_boto.handler
    mock_glue.start_job_run.reset_mock()

    result = handler({"Records": []}, None)

    mock_glue.start_job_run.assert_not_called()
    assert result["statusCode"] == 200
