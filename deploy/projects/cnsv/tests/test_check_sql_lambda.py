"""
Unit tests for deploy/projects/cnsv/lambda/check-sql/lambda_function.py.

Validates:
- SQL file discovery by case-insensitive path resolution.
- Missing file reporting.
- Markdown report upload to the configured artifact bucket.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch


def _load_module():
    module_path = (
        Path(__file__).resolve().parents[1]
        / "lambda"
        / "check-sql"
        / "lambda_function.py"
    )
    spec = importlib.util.spec_from_file_location("cnsv_check_sql_lambda", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to import module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _list_response(keys: list[str]) -> dict:
    return {"Contents": [{"Key": k} for k in keys]}


def test_lambda_handler_reports_found_and_missing_sql_files_and_uploads_report():
    mod = _load_module()

    event = {
        "env": "FPACDEV",
        "data_src_nm": "cnsv",
        "run_type": "incremental",
        "start_date": "2026-01-27",
        "plan": {"stgTables": ["CNSV_TABLE_OK", "CNSV_TABLE_MISSING"]},
    }

    s3 = MagicMock()

    # list_objects_v2 is called once per resolver use.
    # For CNSV_TABLE_OK: base -> table -> run_type -> sql file
    # For CNSV_TABLE_MISSING: base -> table -> run_type -> sql file (missing)
    s3.list_objects_v2.side_effect = [
        _list_response(["cnsv/_configs/STG/CNSV_TABLE_OK/"]),
        _list_response(["cnsv/_configs/STG/CNSV_TABLE_OK/Incremental/"]),
        _list_response(["cnsv/_configs/STG/CNSV_TABLE_OK/Incremental/CNSV_TABLE_OK.SQL"]),
        _list_response(["cnsv/_configs/STG/CNSV_TABLE_MISSING/"]),
        _list_response(["cnsv/_configs/STG/CNSV_TABLE_MISSING/Incremental/"]),
        _list_response([]),
    ]

    # head_object is only called for found files.
    s3.head_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}

    fixed_ts = "2026-03-17T120000"

    mock_datetime = MagicMock()
    mock_datetime.utcnow.return_value.strftime.return_value = fixed_ts

    with (
        patch.object(mod.boto3, "client", return_value=s3),
        patch.object(mod.os, "environ", {"ARTIFACT_BUCKET": "fsa-dev-ops"}),
        patch.object(mod, "datetime", mock_datetime),
    ):

        result = mod.lambda_handler(event, None)

    assert len(result["found"]) == 1
    assert result["found"][0].endswith("CNSV_TABLE_OK/Incremental/CNSV_TABLE_OK.SQL")

    assert len(result["missing"]) == 1
    assert result["missing"][0].startswith("CNSV_TABLE_MISSING")

    assert result["percent_found"] == 50.0
    assert result["report_s3"] == "s3://fsa-dev-ops/report/cnsv/exec-sql-2026-03-17T120000.md"

    s3.put_object.assert_called_once()
    kwargs = s3.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "fsa-dev-ops"
    assert kwargs["Key"] == "report/cnsv/exec-sql-2026-03-17T120000.md"

    report_text = kwargs["Body"].decode("utf-8")
    assert "# EXEC-SQL Preflight Report" in report_text
    assert "## Found (1)" in report_text
    assert "## Missing (1)" in report_text


def test_lambda_handler_returns_error_when_required_parameters_missing():
    mod = _load_module()
    result = mod.lambda_handler({}, None)
    assert result == {"error": "Missing required parameters"}
