import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as apigw from "aws-cdk-lib/aws-apigateway";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as transfer from "aws-cdk-lib/aws-transfer";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as path from "path";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";

export class DartSftpStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // ---- Requested constants ----
    const USERNAME = "S_DART-CERT";
    const PASSWORD = "MyDartTest";
    const HOME_PREFIX = "s_dart_cert";
    const MAIN_SECRET_NAME = "FSA-CERT-Secrets";

    // ============================================================
    // S3 bucket used by Transfer Family
    // ============================================================
    const sftpBucket = new s3.Bucket(this, "DartSftpBucket", {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN
    });

    // ============================================================
    // Role assumed by Transfer Family to access S3
    // Least-privilege: list bucket + R/W/D inside the HOME_PREFIX only
    // ============================================================
    // Home directory prefix inside the bucket
    const homePrefix = "s_dart_cert";

    // Role assumed by AWS Transfer Family to access S3
    const transferS3Role = new iam.Role(this, "TransferS3Role", {
      assumedBy: new iam.ServicePrincipal("transfer.amazonaws.com"),
      description: "S3 access role for AWS Transfer Family users",
    });

    // Required so Transfer can resolve bucket region
    transferS3Role.addToPolicy(new iam.PolicyStatement({
      actions: ["s3:GetBucketLocation"],
      resources: [sftpBucket.bucketArn],
    }));

    // CRITICAL: allow directory listing for ALL prefix variants
    transferS3Role.addToPolicy(new iam.PolicyStatement({
      actions: ["s3:ListBucket"],
      resources: [sftpBucket.bucketArn],
      conditions: {
        StringLike: {
          "s3:prefix": [
            homePrefix,
            `${homePrefix}/`,
            `${homePrefix}/*`,
          ],
        },
      },
    }));

    // Object access within the home directory
    transferS3Role.addToPolicy(new iam.PolicyStatement({
      actions: [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:GetObjectVersion",
        "s3:DeleteObjectVersion",
      ],
      resources: [sftpBucket.arnForObjects(`${homePrefix}/*`)],
    }));

    // Create a folder marker so GUI clients (Cyberduck) can list the directory
    new s3deploy.BucketDeployment(this, "CreateHomePrefixMarker", {
      destinationBucket: sftpBucket,
      destinationKeyPrefix: "s_dart_cert", // ensures prefix exists
      sources: [s3deploy.Source.data(".keep", "")], // creates s_dart_cert/.keep (0-byte)
      retainOnDelete: false,
    });

    // ============================================================
    // Custom Identity Provider (API Gateway -> Lambda)
    // Password auth requires custom IdP for Transfer Family SFTP
    // ============================================================
    const idpLambda = new lambda.Function(this, "TransferIdpLambda", {
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "..", "lambda", "transfer-idp")),
      timeout: cdk.Duration.seconds(10),
      environment: {
        USER_SECRET_PREFIX: "transfer"
      }
    });

    idpLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ["secretsmanager:GetSecretValue"],
      resources: ["*"] // tighten after deploy if you want exact ARNs
    }));

    const api = new apigw.LambdaRestApi(this, "TransferIdpApi", {
      handler: idpLambda,
      proxy: true,
      deployOptions: { stageName: "prod" }
    });

    const invokeRole = new iam.Role(this, "TransferInvokeRole", {
      assumedBy: new iam.ServicePrincipal("transfer.amazonaws.com")
    });

    invokeRole.addToPolicy(new iam.PolicyStatement({
      actions: ["execute-api:Invoke"],
      resources: [api.arnForExecuteApi("*")]
    }));

    // ============================================================
    // Transfer Family SFTP Server (PUBLIC)
    // ============================================================
    const server = new transfer.CfnServer(this, "DartSftpServer", {
      endpointType: "PUBLIC",
      protocols: ["SFTP"],
      domain: "S3",
      identityProviderType: "API_GATEWAY",
      identityProviderDetails: {
        url: api.url,
        invocationRole: invokeRole.roleArn
      }
    });

    // DNS format: s-<serverId>.server.transfer.<region>.amazonaws.com
    const sftpDnsName = cdk.Fn.sub(
      "s-${ServerId}.server.transfer.${AWS::Region}.amazonaws.com",
      { ServerId: server.attrServerId }
    );

    // ============================================================
    // Per-user secret consumed by the IdP Lambda:
    // transfer/<serverId>/<username>
    // ============================================================
    const userSecret = new secretsmanager.Secret(this, "TransferUserSecret", {
      secretName: cdk.Fn.sub("transfer/${ServerId}/${User}", {
        ServerId: server.attrServerId,
        User: USERNAME
      }),
      secretStringValue: cdk.SecretValue.unsafePlainText(JSON.stringify({
        Password: PASSWORD,
        Role: transferS3Role.roleArn,
        HomeDirectory: `/${sftpBucket.bucketName}/${HOME_PREFIX}`
      }))
    });
    userSecret.node.addDependency(server);

    // ============================================================
    // Main secrets JSON: FSA-CERT-Secrets
    // echo_ip is the SFTP DNS name, port is 22
    // ============================================================
const fsaSecretJson =
  `{
  "echo_ip": "${sftpDnsName}",
  "echo_port": "22",
  "echo_dart_path": "${HOME_PREFIX}",
  "echo_dart_password": "${PASSWORD}",
  "echo_dart_username": "${USERNAME}",
  "flpidsload_cert_lambda": "FSA-CERT-DART-ECHO-FETCH",
  "flpidsrpt_cert_lambda": "FSA-CERT-DART-ECHO-FETCH",
  "flpidscaorpt_cert_lambda": "FSA-CERT-DART-ECHO-FETCH",
  "flpidscaorpt_congdist_cert_lambda": "FSA-CERT-DART-ECHO-FETCH",
  "flpidscaorpt_organization_cert_lambda": "FSA-CERT-DART-ECHO-FETCH",
  "flpidsgoalrpt_cert_lambda": "FSA-CERT-DART-ECHO-FETCH",
  "flpidsofcrpt_cert_lambda": "FSA-CERT-DART-ECHO-FETCH",
  "flpidsscims_cert_lambda": "FSA-CERT-DART-ECHO-FETCH",
  "flpidsnats_cert_lambda": "FSA-CERT-DART-ECHO-FETCH",
  "mssql_flpids_ip": "199.134.88.16\\t\\t"
}`;


    new secretsmanager.Secret(this, "FsaCertSecrets", {
      secretName: MAIN_SECRET_NAME,
      secretStringValue: cdk.SecretValue.unsafePlainText(fsaSecretJson)
    });

    // ============================================================
    // Health Check: Scheduled Lambda does TCP connect to <dns>:22
    // Publishes custom metric FSA/TransferHealth:SftpPort22 (value 1/0)
    // Alarm if value < 1 for 2 consecutive periods
    // ============================================================
    const healthFn = new lambda.Function(this, "SftpHealthCheckFn", {
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "..", "lambda", "health-check")),
      timeout: cdk.Duration.seconds(10),
      environment: {
        SFTP_HOST: sftpDnsName,
        SFTP_PORT: "22",
        METRIC_NAMESPACE: "FSA/TransferHealth",
        METRIC_NAME: "SftpPort22",
        SERVER_ID: server.attrServerId
      }
    });

    // PutMetricData permission (namespace is enforced in code; IAM can't scope by namespace)
    healthFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["cloudwatch:PutMetricData"],
      resources: ["*"]
    }));

    // Run every 5 minutes
    const rule = new events.Rule(this, "SftpHealthSchedule", {
      schedule: events.Schedule.rate(cdk.Duration.minutes(5))
    });
    rule.addTarget(new targets.LambdaFunction(healthFn));

    // Alarm on custom metric
    const healthMetric = new cloudwatch.Metric({
      namespace: "FSA/TransferHealth",
      metricName: "SftpPort22",
      period: cdk.Duration.minutes(5),
      statistic: "Minimum",
      dimensionsMap: {
        ServerId: server.attrServerId
      }
    });

    new cloudwatch.Alarm(this, "SftpHealthAlarm", {
      metric: healthMetric,
      threshold: 1,
      evaluationPeriods: 2,
      datapointsToAlarm: 2,
      comparisonOperator: cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.BREACHING,
      alarmDescription: "SFTP health check failed (TCP connect to port 22) for 2 consecutive periods."
    });


    //************** NWE API Gateway for JenkinsWebHook */

    const jenkinsWebHookFn = new lambda.Function(this, "JenkinsWebHook", {
      functionName: "JenkinsWebHook",
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "..", "lambda", "JenkinsWebHook")),
      timeout: cdk.Duration.seconds(10),
    });

    const webhookApi = new apigw.RestApi(this, "JenkinsWebHookApi", {
      restApiName: "JenkinsWebHookApi",
      deployOptions: { stageName: "prod" },
    });

    const jenkins = webhookApi.root.addResource("jenkins-webhook");
    jenkins.addMethod("POST", new apigw.LambdaIntegration(jenkinsWebHookFn), {
      authorizationType: apigw.AuthorizationType.NONE,
      apiKeyRequired: false,
    });




    // ---- Outputs ----
    new cdk.CfnOutput(this, "SftpEndpoint", { value: sftpDnsName });
    new cdk.CfnOutput(this, "SftpPort", { value: "22" });
    new cdk.CfnOutput(this, "SftpBucket", { value: sftpBucket.bucketName });
    new cdk.CfnOutput(this, "SftpHomePrefix", { value: HOME_PREFIX });

    new cdk.CfnOutput(this, "JenkinsWebHookUrl", {
        value: `${webhookApi.url}jenkins-webhook`,
    });

  }
}
