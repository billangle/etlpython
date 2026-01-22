# FSA-FileChecks: 3 Lambdas + Step Function (ASL) — v2 (matches your DynamoDB row)

This zip contains **only**:
- 3 Lambda functions (Python)
- 1 Step Functions state machine definition (Amazon States Language JSON)
- 1 example execution input

## DynamoDB row (as shown in your screenshot)

Keys:
- `jobId` (PK)
- `project` (SK)

File map:
- `file.fileName`
- `file.fileSize`
- `file.remotePath`
- `file.target`

Stats map:
- `stats.ftpsServer`
- `stats.ftpsPort`
- `stats.checkedPath`
- ...

## Important: Secret id comes from the Step Function input

Your DynamoDB row does NOT include `secret_id`, so the **TransferFile** lambda expects it via:
- Step Function input: `secret_id`
  OR
- Lambda env var: `SECRET_ID`

Secrets Manager secret must contain:
- `echo_dart_username`
- `echo_dart_password`

## Step Function placeholders

`stepfunctions/state_machine.asl.json` contains placeholders:
- `${SetRunningLambdaArn}`
- `${TransferFileLambdaArn}`
- `${FinalizeJobLambdaArn}`

## Minimal execution input

See `examples/start_execution_input.json`

Required fields:
- `jobId`
- `project`
- `table_name`
- `bucket`
- `secret_id`

Optional:
- `verify_tls` (default false)
- `timeout_seconds` (default 120)
- `debug` (default false)
