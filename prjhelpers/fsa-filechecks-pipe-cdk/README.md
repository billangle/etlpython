# FSA FileChecks EventBridge Pipe (CDK)

Creates an **EventBridge Pipe** that triggers a **Step Functions** state machine when a DynamoDB Stream record indicates a job is ready.

## Assumptions
- DynamoDB stream ARN (existing):
  - `arn:aws:dynamodb:us-east-1:335965711887:table/FSA-FileChecks/stream/2026-01-23T14:33:41.288`
- Step Functions state machine ARN (existing):
  - `arn:aws:states:us-east-1:335965711887:stateMachine:FSA-steam-dev-FpacFLPIDS-FileChecks`

## What this stack creates
- EventBridge Pipe: `FSA-FileChecks-Stream-to-StepFunctions`
- IAM role for the pipe (assumed by `pipes.amazonaws.com`)
- CloudWatch log group for the pipe

## Filtering
The pipe only forwards DynamoDB stream events when:
- `eventName` is `INSERT` or `MODIFY`
- `dynamodb.NewImage.status.S` equals `"TRIGGERED"`

## Step Functions Input
By default we pass:
- `jobId` = `NewImage.jobId.S`
- `project` = `NewImage.project.S`

Plus constants:
- `table_name` = `FSA-FileChecks`
- `bucket` = `punkdev-fpacfsa-landing-zone`
- `secret_id` = `FSA-CERT-Secrets`

Edit `inputTemplate` in `lib/fsa-filechecks-pipe-stack.ts` if you want values to come from the DynamoDB row.

## Deploy
```bash
npm install
npm run build
npm run synth
npm run deploy
```

> If needed (one-time):
> `cdk bootstrap aws://335965711887/us-east-1`
