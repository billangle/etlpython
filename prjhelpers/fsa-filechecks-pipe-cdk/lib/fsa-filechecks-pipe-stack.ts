import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as pipes from 'aws-cdk-lib/aws-pipes';
import * as logs from 'aws-cdk-lib/aws-logs';

/**
 * Creates an EventBridge Pipe:
 *   DynamoDB Stream (FSA-FileChecks) -> Step Functions state machine execution
 *
 * Assumptions:
 * - The DynamoDB table and stream already exist.
 * - The Step Functions state machine already exists.
 * - You only want the pipe and its IAM role/logging created here.
 */
export class FsaFileChecksPipeStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const dynamodbStreamArn =
      'arn:aws:dynamodb:us-east-1:335965711887:table/FSA-FileChecks/stream/2026-01-23T14:33:41.288';

    const stateMachineArn =
      'arn:aws:states:us-east-1:335965711887:stateMachine:FSA-steam-dev-FpacFLPIDS-FileChecks';

    // CloudWatch Logs for the pipe (helps debug filter/mapping issues)
    const pipeLogGroup = new logs.LogGroup(this, 'PipeLogGroup', {
      logGroupName: `/aws/pipes/FSA-FileChecks-to-SFN`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Role assumed by EventBridge Pipes
    const pipeRole = new iam.Role(this, 'PipeRole', {
      assumedBy: new iam.ServicePrincipal('pipes.amazonaws.com'),
      description: 'Role for EventBridge Pipe: DynamoDB Stream -> Step Functions',
    });

    // Permissions to read from DynamoDB stream
    pipeRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          'dynamodb:DescribeStream',
          'dynamodb:GetRecords',
          'dynamodb:GetShardIterator',
          'dynamodb:ListStreams',
        ],
        resources: [dynamodbStreamArn],
      })
    );

    // Allow StartExecution on the target state machine
    pipeRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['states:StartExecution'],
        resources: [stateMachineArn],
      })
    );

    // Allow pipes to write execution logs
    pipeRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['logs:CreateLogStream', 'logs:PutLogEvents'],
        resources: [pipeLogGroup.logGroupArn, `${pipeLogGroup.logGroupArn}:*`],
      })
    );

    /**
     * Filter pattern:
     * - Only INSERT or MODIFY
     * - Only when NewImage.status.S == "TRIGGERED"
     */
    const filterPattern = JSON.stringify({
      eventName: ['INSERT', 'MODIFY'],
      dynamodb: {
        NewImage: {
          status: { S: ['TRIGGERED'] },
        },
      },
    });

    /**
     * Target input template.
     * Uses Pipes' JSONPath template format: "<$.path>".
     */
    const inputTemplate = JSON.stringify({
      jobId: '<$.dynamodb.NewImage.jobId.S>',
      project: '<$.dynamodb.NewImage.project.S>',
      table_name: 'FSA-FileChecks',
      bucket: 'punkdev-fpacfsa-landing-zone',
      secret_id: 'FSA-CERT-Secrets',
      verify_tls: false,
      timeout_seconds: 120,
      debug: false,
      streamRecord: '<$>',
    });

    new pipes.CfnPipe(this, 'FsaFileChecksToSfnPipe', {
      name: 'FSA-FileChecks-Stream-to-StepFunctions',
      roleArn: pipeRole.roleArn,
      source: dynamodbStreamArn,
      target: stateMachineArn,

      sourceParameters: {
        dynamoDbStreamParameters: {
          startingPosition: 'LATEST',
          batchSize: 1,
          maximumRetryAttempts: 3,
        },
        filterCriteria: {
          filters: [{ pattern: filterPattern }],
        },
      },

      targetParameters: {
        stepFunctionStateMachineParameters: {
          invocationType: 'FIRE_AND_FORGET',
        },
        inputTemplate,
      },

      logConfiguration: {
        cloudwatchLogsLogDestination: { logGroupArn: pipeLogGroup.logGroupArn },
        level: 'INFO',
        includeExecutionData: ['ALL'],
      },
    });
  }
}
