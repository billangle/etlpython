# buildpipe.md — Manually Create the DynamoDB Stream → EventBridge Pipe → Step Functions Trigger (AWS Console)

These instructions walk through **manually implementing** (in the AWS Console) what the CDK stack created:

- An **EventBridge Pipe** that reads from an existing **DynamoDB Stream** and triggers an existing **Step Functions** state machine.
- An **IAM role** for the Pipe (`pipes.amazonaws.com`) with the required permissions.
- A **CloudWatch Log Group** for Pipe logs.
- A **filter** so the Pipe only triggers when `status == TRIGGERED`.

---

## Assumptions (match your deployment)

- **Region:** `us-east-1`
- **Account:** `335965711887`
- **Existing DynamoDB Stream ARN:**
  ```
  arn:aws:dynamodb:us-east-1:335965711887:table/FSA-FileChecks/stream/2026-01-23T14:33:41.288
  ```
- **Existing Step Functions State Machine ARN:**
  ```
  arn:aws:states:us-east-1:335965711887:stateMachine:FSA-steam-dev-FpacFLPIDS-FileChecks
  ```
- The DynamoDB table and state machine already exist.
- You have permissions to create IAM roles, EventBridge Pipes, and CloudWatch Log Groups.

---

## 1) Verify DynamoDB Stream is Enabled

1. Open **AWS Console** → **DynamoDB**
2. Select **Tables**
3. Click **FSA-FileChecks**
4. Go to **Exports and streams** (or **Streams** depending on console version)
5. Confirm:
   - Streams = **Enabled**
   - Stream view type = **NEW_IMAGE** (recommended) or **NEW_AND_OLD_IMAGES**
6. Confirm you see the stream ARN that matches your provided ARN.

> If streams are not enabled, enable them and select **NEW_IMAGE**, then save.

---

## 2) Create the CloudWatch Log Group (for Pipe logs)

1. Open **AWS Console** → **CloudWatch**
2. Left nav → **Logs** → **Log groups**
3. Click **Create log group**
4. Name it:
   ```
   /aws/pipes/FSA-FileChecks-to-SFN
   ```
5. Set retention (recommended):
   - **1 week** (or your organization standard)
6. Create the log group.

---

## 3) Create the IAM Role for EventBridge Pipes

### 3.1 Create the Role

1. Open **AWS Console** → **IAM**
2. Left nav → **Roles**
3. Click **Create role**
4. **Trusted entity type:** AWS service
5. **Use case / Service:** choose **EventBridge** (or search for **Pipes** / **Amazon EventBridge**)
6. When asked for the service principal, ensure it is:
   ```
   pipes.amazonaws.com
   ```
   (Some consoles show “Amazon EventBridge Pipes” directly.)
7. Click **Next**

### 3.2 Attach Permissions (Inline policy recommended)

You will add a single policy that grants:
- DynamoDB Streams read access
- Step Functions StartExecution
- CloudWatch Logs write access

1. On the **Add permissions** step, you can skip attaching managed policies.
2. Finish creating the role with:
   - **Role name:** `FSA-FileChecks-PipeRole` (or your naming standard)
   - Create role.

### 3.3 Add an Inline Policy

1. Open the new role: **IAM → Roles → FSA-FileChecks-PipeRole**
2. Go to **Permissions** tab
3. Click **Add permissions** → **Create inline policy**
4. Choose **JSON** and paste:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadDynamoDBStream",
      "Effect": "Allow",
      "Action": [
        "dynamodb:DescribeStream",
        "dynamodb:GetRecords",
        "dynamodb:GetShardIterator",
        "dynamodb:ListStreams"
      ],
      "Resource": "arn:aws:dynamodb:us-east-1:335965711887:table/FSA-FileChecks/stream/2026-01-23T14:33:41.288"
    },
    {
      "Sid": "StartStepFunctionsExecution",
      "Effect": "Allow",
      "Action": [
        "states:StartExecution"
      ],
      "Resource": "arn:aws:states:us-east-1:335965711887:stateMachine:FSA-steam-dev-FpacFLPIDS-FileChecks"
    },
    {
      "Sid": "WritePipeLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:us-east-1:335965711887:log-group:/aws/pipes/FSA-FileChecks-to-SFN",
        "arn:aws:logs:us-east-1:335965711887:log-group:/aws/pipes/FSA-FileChecks-to-SFN:*"
      ]
    }
  ]
}
```

5. Click **Next**
6. Name the policy: `FSAFileChecksPipeInlinePolicy`
7. Click **Create policy**

---

## 4) Create the EventBridge Pipe (Console)

1. Open **AWS Console** → **Amazon EventBridge**
2. Left nav → **Pipes**
3. Click **Create pipe**

### 4.1 Define Pipe (Name + Source + Target)

- **Name:** `FSA-FileChecks-Stream-to-StepFunctions`
- **Description:** (optional) “Trigger FileChecks Step Function when a job becomes TRIGGERED”
- **Source:** DynamoDB (Streams)
  - Choose **DynamoDB stream ARN**
  - Paste:
    ```
    arn:aws:dynamodb:us-east-1:335965711887:table/FSA-FileChecks/stream/2026-01-23T14:33:41.288
    ```

- **Target:** Step Functions
  - Select **Step Functions state machine**
  - Paste ARN:
    ```
    arn:aws:states:us-east-1:335965711887:stateMachine:FSA-steam-dev-FpacFLPIDS-FileChecks
    ```

Click **Next**

### 4.2 Source Parameters (DynamoDB Stream settings)

Configure:

- **Starting position:** `LATEST`
- **Batch size:** `1`
- **Maximum retry attempts:** `3` (or org standard)
- (If present) **Maximum record age**: set per your standard (e.g., 1 hour)

Click **Next** (or continue)

### 4.3 Filtering (Only status == TRIGGERED)

Find **Filter criteria** / **Add filter**.

Paste this filter pattern:

```json
{
  "eventName": ["INSERT", "MODIFY"],
  "dynamodb": {
    "NewImage": {
      "status": {
        "S": ["TRIGGERED"]
      }
    }
  }
}
```

This ensures the pipe triggers only when the stream record’s `NewImage.status.S` equals `"TRIGGERED"`.

Click **Next**

### 4.4 Target Parameters (StartExecution and Input Template)

1. **Invocation type:** `FIRE_AND_FORGET`
2. **Input transformation / Input template**:
   Use this template:

```json
{
  "jobId": "<$.dynamodb.NewImage.jobId.S>",
  "project": "<$.dynamodb.NewImage.project.S>",
  "table_name": "FSA-FileChecks",
  "bucket": "punkdev-fpacfsa-landing-zone",
  "secret_id": "FSA-CERT-Secrets",
  "verify_tls": false,
  "timeout_seconds": 120,
  "debug": false
}
```

> Note: Many Pipe targets deliver records as a **batch array**, even with batch size 1.  
> Your Step Function handles this using the **UnwrapRecord** Pass state (`InputPath: $[0]`).

Click **Next**

### 4.5 Logging

Enable logging (recommended):

- **Log destination:** CloudWatch Logs
- Choose log group:
  ```
  /aws/pipes/FSA-FileChecks-to-SFN
  ```
- **Log level:** `INFO`
- **Include execution data:** `ALL`

Click **Next**

### 4.6 Role

When asked for the role:

- Choose **Use existing role**
- Select: `FSA-FileChecks-PipeRole`

Click **Next** → **Create pipe**

---

## 5) Validate the Pipe

### 5.1 Confirm Pipe Status

1. EventBridge → Pipes
2. Click `FSA-FileChecks-Stream-to-StepFunctions`
3. Confirm status is **Running**

### 5.2 Generate a Trigger Event

Update or insert a DynamoDB item so that:

- `status` becomes `"TRIGGERED"`
- `jobId` and `project` exist in `NewImage`

Example:
- In DynamoDB Console → Explore items → Edit item → set `status = TRIGGERED` → Save

### 5.3 Confirm Step Function Execution Started

1. Open **AWS Console** → **Step Functions**
2. Select state machine:
   - `FSA-steam-dev-FpacFLPIDS-FileChecks`
3. Go to **Executions**
4. Confirm a new execution started around the time you updated DynamoDB.

---

## 6) Troubleshooting

### 6.1 Pipe shows failures / no executions

- Check CloudWatch log group:
  ```
  /aws/pipes/FSA-FileChecks-to-SFN
  ```
- Common causes:
  - Filter pattern doesn’t match (status stored as `S` vs `N` etc.)
  - Wrong Stream ARN (stream ARN changes if stream is disabled/enabled)
  - Pipe role lacks permission

### 6.2 AccessDenied starting Step Function

Update the Pipe role policy to ensure:

- `states:StartExecution` is allowed on the state machine ARN

### 6.3 Step Function fails immediately with missing `jobId` / `project`

This happens if the Pipe input is a batch array and the state machine expects an object.

Fix:
- Ensure your state machine starts with:

```json
"UnwrapRecord": {
  "Type": "Pass",
  "InputPath": "$[0]",
  "ResultPath": "$",
  "Next": "SetRunning"
}
```

---

## 7) What You Built (Console Equivalent of CDK)

✅ CloudWatch log group for Pipe logs  
✅ IAM Role for Pipes (`pipes.amazonaws.com`)  
✅ EventBridge Pipe with:
- DynamoDB stream source
- filter `status == TRIGGERED`
- Step Functions target `StartExecution`
- input template mapping from DynamoDB stream NewImage

---

## Appendix: Quick Reference Values

- Stream ARN:
  ```
  arn:aws:dynamodb:us-east-1:335965711887:table/FSA-FileChecks/stream/2026-01-23T14:33:41.288
  ```
- State machine ARN:
  ```
  arn:aws:states:us-east-1:335965711887:stateMachine:FSA-steam-dev-FpacFLPIDS-FileChecks
  ```
- Log group:
  ```
  /aws/pipes/FSA-FileChecks-to-SFN
  ```
- Pipe name:
  ```
  FSA-FileChecks-Stream-to-StepFunctions
  ```
