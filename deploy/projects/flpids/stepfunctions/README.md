# FSA FileChecks – Event-Driven FTPS File Ingestion Pipeline

## Overview

This solution implements an **event-driven, fully automated file ingestion pipeline** for FPAC/FSA using:

- **DynamoDB** as the system of record (`FSA-FileChecks`)
- **DynamoDB Streams + EventBridge Pipes** to trigger processing
- **AWS Step Functions** for orchestration
- **AWS Lambda** for execution logic
- **FTPS (via curl)** as the source system
- **Amazon S3** as the landing zone

The pipeline is **idempotent**, **self-healing**, and **auditable**, and requires **no polling** or manual triggering.

---

## High-Level Flow

DynamoDB (FSA-FileChecks)  
→ DynamoDB Stream  
→ EventBridge Pipe  
→ Step Functions  
→ Lambdas (SetRunning → TransferFile → FinalizeJob)

---

## DynamoDB Table: `FSA-FileChecks`

**Primary Keys**
- Partition key: `jobId`
- Sort key: `project`

### Example Completed Item

```json
{
  "jobId": "5fa16210-84a8-4442-a17b-059f290b1b5c",
  "project": "FLPIDS-NATS",
  "pipeline": "FLPIDS_ODS",
  "status": "COMPLETED",
  "createdAt": "2026-01-23T14:58:59.051433+00:00",
  "startedAt": "2026-01-23T14:59:01.591206+00:00",
  "completedAt": "2026-01-23T14:59:08.357292+00:00",
  "updatedAt": "2026-01-23T14:59:08.357292+00:00",
  "transferStage": "DONE",
  "file": {
    "fileName": "mo.moyr540.data",
    "remotePath": "/s_dart_cert/plas/in/mo.moyr540.data"
  },
  "s3": {
    "bucket": "punkdev-fpacfsa-landing-zone",
    "key": "FLPIDS-NATS/mo.moyr540.data"
  }
}
```

---

## Event Triggering

### DynamoDB Stream
- Enabled with **NEW_IMAGE**
- Emits events when rows are inserted or modified

### EventBridge Pipe
- Filters on `status = TRIGGERED`
- Starts Step Function:
  ```
  arn:aws:states:us-east-1:335965711887:stateMachine:FSA-steam-dev-FpacFLPIDS-FileChecks
  ```

The Pipe delivers events as a **single-element array**, which is handled in the state machine.

---

## Step Functions State Machine

### Name
`FSA-steam-dev-FpacFLPIDS-FileChecks`

### Execution Example
```
Execution ID: f3900b4f-020b-4b32-9458-4e628a487153
Execution Type: Standard
```

### State Flow

1. **UnwrapRecord (Pass)** – converts `[ { ... } ]` → `{ ... }`
2. **SetRunning (Lambda)** – sets status to RUNNING
3. **TransferFile (Lambda)** – FTPS → S3 → delete source
4. **FinalizeJob (Lambda)** – COMPLETED or ERROR

---

## Lambda Responsibilities

### SetRunning
- Updates DynamoDB status to `RUNNING`
- Sets `startedAt`

### TransferFile
- Reads FTPS credentials from Secrets Manager
- Downloads file via FTPS
- Uploads to:
  ```
  s3://<bucket>/<project>/<filename>
  ```
- Deletes remote file
- Updates transfer metadata

### FinalizeJob
- On success: sets `COMPLETED`
- On failure: sets `ERROR` with error details

---

## Error Handling

- Any failure in TransferFile is caught
- FinalizeJobOnCatch updates DynamoDB with error info
- Full execution history is preserved in Step Functions

---

## IAM Summary

**Lambda role requires:**
- DynamoDB `GetItem`, `UpdateItem`
- Secrets Manager `GetSecretValue`
- S3 `PutObject` on landing bucket

**Step Functions role requires:**
- `lambda:InvokeFunction`

**EventBridge Pipe role requires:**
- DynamoDB Streams read
- `states:StartExecution`

---

## Why This Architecture

- No polling
- Deterministic, event-driven execution
- Clear lifecycle tracking in DynamoDB
- Minimal operational overhead
- FPAC-aligned least-privilege design

---

## Status

✅ Deployed  
✅ Event-driven  
✅ Files transferred successfully  
✅ DynamoDB reflects full lifecycle  
