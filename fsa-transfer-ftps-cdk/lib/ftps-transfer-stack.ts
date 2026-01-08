import * as path from "path";
import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as apigw from "aws-cdk-lib/aws-apigateway";
import * as transfer from "aws-cdk-lib/aws-transfer";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as route53 from "aws-cdk-lib/aws-route53";
import * as acm from "aws-cdk-lib/aws-certificatemanager";
import * as cr from "aws-cdk-lib/custom-resources";
import * as events from "aws-cdk-lib/aws-events";
import * as eventTargets from "aws-cdk-lib/aws-events-targets";

type Ctx = {
  deployEnv: string;
  projectName: string;
  homePrefix: string;
  transferUserName: string;
  transferUserPassword: string;
  passivePortStart: number;
  passivePortEnd: number;
  ftpsDomainName: string;
  hostedZoneDomain: string;
};

export class FtpsTransferStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const get = (k: string) => this.node.tryGetContext(k);

    const cfg: Ctx = {
      deployEnv: String(get("deployEnv") ?? "CERT"),
      projectName: String(get("projectName") ?? "DART-ECHO-FETCH"),
      homePrefix: String(get("homePrefix") ?? "s_dart_cert"),
      transferUserName: String(get("transferUserName") ?? "S_DART-CERT"),
      transferUserPassword: String(get("transferUserPassword") ?? "MyDartTest"),
      passivePortStart: Number(get("passivePortStart") ?? 8192),
      passivePortEnd: Number(get("passivePortEnd") ?? 8200),
      ftpsDomainName: "ftps.steamfpac.com", // HARDCODED
      hostedZoneDomain: "steamfpac.com" // HARDCODED
    };
    // 1) S3 bucket used by Transfer
    const transferBucket = new s3.Bucket(this, "TransferBucket", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true
    });

    // Create a marker object so GUI clients can list the prefix
    new s3deploy.BucketDeployment(this, "CreateHomePrefixMarker", {
      destinationBucket: transferBucket,
      destinationKeyPrefix: cfg.homePrefix,
      sources: [s3deploy.Source.data(".keep", "")],
      retainOnDelete: false
    });

    // 2) IAM role for Transfer to access S3 within home prefix
    const transferS3Role = new iam.Role(this, "TransferS3Role", {
      assumedBy: new iam.ServicePrincipal("transfer.amazonaws.com"),
      description: "Role assumed by AWS Transfer Family users to access S3"
    });

    transferS3Role.addToPolicy(new iam.PolicyStatement({
      actions: ["s3:GetBucketLocation"],
      resources: [transferBucket.bucketArn]
    }));

    transferS3Role.addToPolicy(new iam.PolicyStatement({
      actions: ["s3:ListBucket"],
      resources: [transferBucket.bucketArn],
      conditions: {
        StringLike: {
          "s3:prefix": [
            cfg.homePrefix,
            `${cfg.homePrefix}/`,
            `${cfg.homePrefix}/*`
          ]
        }
      }
    }));

    transferS3Role.addToPolicy(new iam.PolicyStatement({
      actions: [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:GetObjectVersion",
        "s3:DeleteObjectVersion"
      ],
      resources: [transferBucket.arnForObjects(`${cfg.homePrefix}/*`)]
    }));

    // 3) VPC + Security Group for FTPS internet-facing endpoint
    const vpc = new ec2.Vpc(this, "TransferVpc", { natGateways: 1 });

    const ftpsSg = new ec2.SecurityGroup(this, "TransferFtpsSg", {
      vpc,
      allowAllOutbound: true,
      description: "SG for AWS Transfer Family FTPS"
    });

    // TODO: restrict to your CIDRs
    ftpsSg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(21), "FTPS control");
    ftpsSg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcpRange(cfg.passivePortStart, cfg.passivePortEnd), "FTPS passive ports");

    const eip1 = new ec2.CfnEIP(this, "TransferEip1", { domain: "vpc" });

    // ACM cert created in CDK via DNS validation in Route53
    

// DNS + ACM (fully in-CDK, no synth-time lookups):
// Ensure a public hosted zone exists (create if missing) using a deploy-time custom resource.
const hostedZoneProviderFn = new lambda.Function(this, "HostedZoneProvider", {
  runtime: lambda.Runtime.NODEJS_22_X,
  handler: "index.handler",
  code: lambda.Code.fromAsset(path.join(__dirname, "..", "lambda", "HostedZoneProvider")),
  timeout: cdk.Duration.seconds(30),
  memorySize: 256
});

hostedZoneProviderFn.addToRolePolicy(new iam.PolicyStatement({
  actions: [
    "route53:ListHostedZonesByName",
    "route53:CreateHostedZone",
    "route53:GetHostedZone",
    "route53:DeleteHostedZone"
  ],
  resources: ["*"]
}));

const zoneCr = new cr.Provider(this, "HostedZoneCrProvider", {
  onEventHandler: hostedZoneProviderFn
});

const ensureZone = new cdk.CustomResource(this, "EnsurePublicHostedZone", {
  serviceToken: zoneCr.serviceToken,
  properties: {
    ZoneName: cfg.hostedZoneDomain,
    DeleteOnRemove: false
  }
});

const hostedZoneId = ensureZone.getAttString("HostedZoneId");

// Create DNS record ftpsDomainName -> EIP using L1 (hostedZoneId is a deploy-time token)
const recordLabel = cfg.ftpsDomainName.endsWith(`.${cfg.hostedZoneDomain}`)
  ? cfg.ftpsDomainName.slice(0, -(cfg.hostedZoneDomain.length + 1))
  : cfg.ftpsDomainName;

new route53.CfnRecordSet(this, "FtpsARecord", {
  hostedZoneId,
  name: `${recordLabel}.${cfg.hostedZoneDomain}.`,
  type: "A",
  ttl: "300",
  resourceRecords: [eip1.ref]
});

// Create ACM cert (DNS validation) using L1 + hostedZoneId
const cert = new acm.CfnCertificate(this, "FtpsCertificate", {
  domainName: cfg.ftpsDomainName,
  validationMethod: "DNS",
  domainValidationOptions: [{
    domainName: cfg.ftpsDomainName,
    hostedZoneId
  }]
});

// 4) Custom Identity Provider API (API Gateway + Lambda Node 22 index.mjs)
    const idpFn = new lambda.Function(this, "TransferIdpLambda", {
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "..", "lambda", "TransferIdp")),
      timeout: cdk.Duration.seconds(10),
      memorySize: 256,
      environment: { USER_SECRET_PREFIX: "transfer" }
    });

    // Needs to read secrets (per-user secret + env secret)
    idpFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
      resources: ["*"]
    }));

    const idpApi = new apigw.RestApi(this, "TransferIdpApi", {
      restApiName: "TransferIdpApi",
      deployOptions: { stageName: "prod" },
      endpointTypes: [apigw.EndpointType.REGIONAL]
    });

    idpApi.root.addProxy({
      defaultIntegration: new apigw.LambdaIntegration(idpFn, { proxy: true }),
      anyMethod: true
    });

    // Invocation role that Transfer assumes to call API Gateway
    
// Secrets provider (create-or-update secrets with fixed names to avoid deployment failures when they already exist)
const secretsProviderFn = new lambda.Function(this, "SecretsProviderFn", {
  runtime: lambda.Runtime.NODEJS_22_X,
  handler: "index.handler",
  code: lambda.Code.fromAsset(path.join(__dirname, "..", "lambda", "SecretsProvider")),
  timeout: cdk.Duration.seconds(30),
  memorySize: 256
});

secretsProviderFn.addToRolePolicy(new iam.PolicyStatement({
  actions: [
    "secretsmanager:DescribeSecret",
    "secretsmanager:CreateSecret",
    "secretsmanager:PutSecretValue",
    "secretsmanager:TagResource"
  ],
  resources: ["*"]
}));

const secretsCr = new cr.Provider(this, "SecretsCrProvider", {
  onEventHandler: secretsProviderFn
});

const transferInvokeRole = new iam.Role(this, "TransferInvokeRole", {
      assumedBy: new iam.ServicePrincipal("transfer.amazonaws.com")
    });

    transferInvokeRole.addToPolicy(new iam.PolicyStatement({
      actions: ["execute-api:Invoke"],
      resources: [idpApi.arnForExecuteApi("*", "/*", "prod")]
    }));

    // 5) Jenkins Webhook echo endpoint (UNSECURED POST) on same API
    const jenkinsFn = new lambda.Function(this, "JenkinsWebHook", {
            runtime: lambda.Runtime.NODEJS_22_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "..", "lambda", "JenkinsWebHook")),
      timeout: cdk.Duration.seconds(10)
    });

    const jenkins = idpApi.root.addResource("jenkins-webhook");
    jenkins.addMethod("POST", new apigw.LambdaIntegration(jenkinsFn, { proxy: true }), {
      authorizationType: apigw.AuthorizationType.NONE,
      apiKeyRequired: false
    });

    // FTPS server
    const server = new transfer.CfnServer(this, "FtpsServer", {
      protocols: ["FTPS"],
      endpointType: "VPC",
      certificate: cert.ref,
      domain: "S3",
      identityProviderType: "API_GATEWAY",
      identityProviderDetails: {
        url: idpApi.url,
        invocationRole: transferInvokeRole.roleArn
      },
      endpointDetails: {
        vpcId: vpc.vpcId,
        subnetIds: [vpc.publicSubnets[0].subnetId], // single subnet to match single EIP
        securityGroupIds: [ftpsSg.securityGroupId],
        addressAllocationIds: [eip1.attrAllocationId]
      },
      protocolDetails: { tlsSessionResumptionMode: "ENFORCED" }
    });

    // Per-user secret used by the IdP lambda
    const homeDetails = JSON.stringify([
      { Entry: "/", Target: `/${transferBucket.bucketName}/${cfg.homePrefix}` }
    ]);


const transferUserSecretName = `transfer/${server.attrServerId}/${cfg.transferUserName}`;

const ensureTransferUserSecret = new cdk.CustomResource(this, "EnsureTransferUserSecret", {
  serviceToken: secretsCr.serviceToken,
  properties: {
    Name: transferUserSecretName,
    SecretString: JSON.stringify({
      Password: cfg.transferUserPassword,
      Role: transferS3Role.roleArn,
      HomeDirectoryDetails: homeDetails
    }),
    Tags: [{ Key: "ManagedBy", Value: "CDK" }]
  }
});

const fsaEnvSecretName = `FSA-${cfg.deployEnv}-Secrets`;

const ensureFsaEnvSecrets = new cdk.CustomResource(this, "EnsureFsaEnvSecrets", {
  serviceToken: secretsCr.serviceToken,
  properties: {
    Name: fsaEnvSecretName,
    SecretString: JSON.stringify({
      echo_ip: cfg.ftpsDomainName,
      echo_port: "21",
      echo_dart_path: cfg.homePrefix,
      echo_dart_password: cfg.transferUserPassword,
      echo_dart_username: cfg.transferUserName,
      flpidsload_cert_lambda: `FSA-${cfg.deployEnv}-DART-ECHO-FETCH`,
      flpidsrpt_cert_lambda: `FSA-${cfg.deployEnv}-DART-ECHO-FETCH`,
      flpidscaorpt_cert_lambda: `FSA-${cfg.deployEnv}-DART-ECHO-FETCH`,
      flpidscaorpt_congdist_cert_lambda: `FSA-${cfg.deployEnv}-DART-ECHO-FETCH`,
      flpidscaorpt_organization_cert_lambda: `FSA-${cfg.deployEnv}-DART-ECHO-FETCH`,
      flpidsgoalrpt_cert_lambda: `FSA-${cfg.deployEnv}-DART-ECHO-FETCH`,
      flpidsofcrpt_cert_lambda: `FSA-${cfg.deployEnv}-DART-ECHO-FETCH`,
      flpidsscims_cert_lambda: `FSA-${cfg.deployEnv}-DART-ECHO-FETCH`,
      flpidsnats_cert_lambda: `FSA-${cfg.deployEnv}-DART-ECHO-FETCH`,
      mssql_flpids_ip: "199.134.88.16"
    }),
    Tags: [{ Key: "ManagedBy", Value: "CDK" }]
  }
});



        // Main app secret: FSA-CERT-Secrets JSON
        // Health check lambda (runs every 5 minutes)
    const healthFn = new lambda.Function(this, "FtpsHealthCheckFn", {
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "..", "lambda", "SftpHealthCheck")),
      timeout: cdk.Duration.seconds(10),
      memorySize: 256,
      environment: {
        FTPS_HOST: cfg.ftpsDomainName,
        FTPS_PORT: "21",
        SERVER_ID: server.attrServerId,
        METRIC_NAMESPACE: "FSA/TransferHealth",
        METRIC_NAME: "FtpsControlPort"
      }
    });

    healthFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["cloudwatch:PutMetricData"],
      resources: ["*"]
    }));

    const rule = new events.Rule(this, "FtpsHealthSchedule", {
      schedule: events.Schedule.rate(cdk.Duration.minutes(5))
    });
    rule.addTarget(new eventTargets.LambdaFunction(healthFn));

    


// Hosted zone info (created/reused at deploy time)
new cdk.CfnOutput(this, "HostedZoneIdOutput", { value: hostedZoneId });
new cdk.CfnOutput(this, "HostedZoneNameServers", { value: ensureZone.getAttString("NameServers") });

// Outputs
    new cdk.CfnOutput(this, "TransferBucketName", { value: transferBucket.bucketName });
    new cdk.CfnOutput(this, "TransferHomePrefixOutput", { value: cfg.homePrefix });
    new cdk.CfnOutput(this, "TransferUserNameOutput", { value: cfg.transferUserName });
    new cdk.CfnOutput(this, "FtpsPublicIpOutput", { value: eip1.ref });
    new cdk.CfnOutput(this, "FtpsHostnameOutput", { value: cfg.ftpsDomainName });
    new cdk.CfnOutput(this, "FtpsPortOutput", { value: "21" });

    new cdk.CfnOutput(this, "TransferIdpApiUrlOutput", { value: idpApi.url });
    new cdk.CfnOutput(this, "JenkinsWebHookUrlOutput", { value: `${idpApi.url}jenkins-webhook` });

    new cdk.CfnOutput(this, "SecretNameOutput", { value: fsaEnvSecretName });
    new cdk.CfnOutput(this, "TransferUserSecretNameOutput", { value: transferUserSecretName });
  }
}
