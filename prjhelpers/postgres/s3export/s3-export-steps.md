# Amazon RDS for PostgreSQL -- Export Data to S3 Using a Stored Procedure

This guide explains how to configure **Amazon RDS for PostgreSQL** to
export query results to Amazon S3 using the `aws_s3` extension and a
stored procedure wrapper.

------------------------------------------------------------------------

## 1. Attach an IAM Role to the RDS Instance

RDS exports to S3 by assuming an IAM role attached to the DB instance.

### Required S3 Permissions (Minimum)

The IAM policy attached to the role should include:

-   `s3:PutObject`
-   `s3:AbortMultipartUpload`
-   `s3:ListBucket`
-   `s3:GetBucketLocation`

### Attach Role to DB Instance (CLI Example)

``` bash
aws rds add-role-to-db-instance   --db-instance-identifier YOUR_DB_INSTANCE_ID   --role-arn arn:aws:iam::123456789012:role/YOUR_RDS_S3_EXPORT_ROLE   --feature-name S3_EXPORT
```

You can also attach the role in the AWS Console: RDS → Databases →
Modify → Manage IAM roles → Add role (Feature: `S3_EXPORT`).

------------------------------------------------------------------------

## 2. Enable Required Extensions

Connect to your RDS PostgreSQL database and run:

``` sql
CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;
CREATE EXTENSION IF NOT EXISTS aws_commons;
```

------------------------------------------------------------------------

## 3. Create a Stored Procedure Wrapper

RDS provides the function `aws_s3.query_export_to_s3` to export query
results to S3.

Create a stored procedure wrapper:

``` sql
CREATE OR REPLACE PROCEDURE export_query_to_s3(
    p_query   text,
    p_bucket  text,
    p_key     text,
    p_region  text DEFAULT 'us-east-1'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_uri aws_commons._s3_uri_1;
    v_rows bigint;
    v_files bigint;
    v_bytes bigint;
BEGIN
    v_uri := aws_commons.create_s3_uri(p_bucket, p_key, p_region);

    SELECT rows_uploaded, files_uploaded, bytes_uploaded
      INTO v_rows, v_files, v_bytes
      FROM aws_s3.query_export_to_s3(
            p_query,
            v_uri,
            options := 'format csv, header true'
          );

    RAISE NOTICE 'Export complete: rows=%, files=%, bytes=%', v_rows, v_files, v_bytes;
END;
$$;
```

------------------------------------------------------------------------

## 4. Call the Stored Procedure

Example usage:

``` sql
CALL export_query_to_s3(
  'SELECT * FROM public.my_table WHERE updated_at >= now() - interval ''1 day''',
  'my-export-bucket',
  'exports/my_table_daily.csv',
  'us-east-1'
);
```

------------------------------------------------------------------------

## 5. Important Notes

### Network Access

If your RDS instance is in private subnets, ensure it can reach S3
(typically via a VPC S3 endpoint).

### Large Exports

Large exports may be split into multiple parts (\~6 GB per file) with
suffixes such as `_partXX`.

### Export Format Options

The `options` parameter supports PostgreSQL COPY options, including:

-   `format csv`
-   `header true`
-   `delimiter ','`
-   `encoding 'UTF8'`

Example:

``` sql
options := 'format csv, delimiter ''|'', header true'
```

------------------------------------------------------------------------

## 6. Security Recommendation

To limit direct access to the `aws_s3` extension:

-   Create the procedure under a privileged role.
-   Restrict direct access to `aws_s3` functions.
-   Grant `EXECUTE` on the procedure only to approved database roles.

Example:

``` sql
GRANT EXECUTE ON PROCEDURE export_query_to_s3(text,text,text,text) TO reporting_role;
```

------------------------------------------------------------------------

## Summary

By attaching an IAM role, enabling the `aws_s3` extension, and wrapping
the export function in a stored procedure, you can safely and
programmatically export PostgreSQL query results from Amazon RDS
directly to Amazon S3.
