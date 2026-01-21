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
import * as logs from "aws-cdk-lib/aws-logs";

export class FtpsTransferStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // ============================================================
    // HARD-CODED CONFIG
    // ============================================================
    const HOSTED_ZONE_DOMAIN = "steamfpac.com";
    const FTPS_FQDN = "ftps.steamfpac.com";

    // ✅ requested allow list
    const ALLOWED_CIDRS: string[] = ["216.234.197.6/32"];

    const PASSIVE_START = 8192;
    const PASSIVE_END = 8200;

    const HOME_PREFIX = "s_dart_cert";
    const TRANSFER_USERNAME = "S_DART-CERT";
    const TRANSFER_PASSWORD = "MyDartTest";

    // ============================================================
    // 0) Import EXISTING public hosted zone from Route53
    // ============================================================
    const hostedZone = route53.HostedZone.fromLookup(this, "SteamFpacHostedZone", {
      domainName: HOSTED_ZONE_DOMAIN,
    });

    // ============================================================
    // 1) VPC + FIXED NAT EIP (Lambda egress)
    // ============================================================
    const lambdaNatEip = new ec2.CfnEIP(this, "LambdaNatEip", { domain: "vpc" });

    const natProvider = ec2.NatProvider.gateway({
      eipAllocationIds: [lambdaNatEip.attrAllocationId],
    });

    const vpc = new ec2.Vpc(this, "TransferVpc", {
      natGateways: 1,
      natGatewayProvider: natProvider,
    });

    const lambdaSubnets = vpc.selectSubnets({ subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS });

    const lambdaClientSg = new ec2.SecurityGroup(this, "LambdaFtpsClientSg", {
      vpc,
      allowAllOutbound: true,
      description: "Attach to Lambdas that must reach ftps.steamfpac.com",
    });

    // ============================================================
    // 2) FTPS Server SG: allow humans + Lambda NAT only
    // ============================================================
    const ftpsSg = new ec2.SecurityGroup(this, "FtpsServerSg", {
      vpc,
      allowAllOutbound: true,
      description: "FTPS allow-list + Lambda NAT allow-list",
    });

    for (const cidr of ALLOWED_CIDRS) {
      ftpsSg.addIngressRule(ec2.Peer.ipv4(cidr), ec2.Port.tcp(21), `FTPS control (${cidr})`);
      ftpsSg.addIngressRule(
        ec2.Peer.ipv4(cidr),
        ec2.Port.tcpRange(PASSIVE_START, PASSIVE_END),
        `FTPS passive (${cidr})`
      );
    }

    // Lambda egress appears as NAT EIP public IP
    const natCidr = cdk.Fn.join("", [lambdaNatEip.ref, "/32"]);
    ftpsSg.addIngressRule(ec2.Peer.ipv4(natCidr), ec2.Port.tcp(21), "FTPS control (Lambda via NAT)");
    ftpsSg.addIngressRule(
      ec2.Peer.ipv4(natCidr),
      ec2.Port.tcpRange(PASSIVE_START, PASSIVE_END),
      "FTPS passive (Lambda via NAT)"
    );

    // ============================================================
    // 3) FTPS EIP + Route53 A record
    // ============================================================
    const ftpsEip = new ec2.CfnEIP(this, "FtpsEip", { domain: "vpc" });

    new route53.ARecord(this, "FtpsARecord", {
      zone: hostedZone,
      recordName: "ftps",
      target: route53.RecordTarget.fromIpAddresses(ftpsEip.ref),
      ttl: cdk.Duration.minutes(5),
    });

    // ============================================================
    // 4) ACM certificate (DNS validation via hosted zone)
    // ============================================================
    const cert = new acm.Certificate(this, "FtpsCertificate", {
      domainName: FTPS_FQDN,
      validation: acm.CertificateValidation.fromDns(hostedZone),
    });

    // ============================================================
    // 5) S3 bucket for Transfer
    // ============================================================
    const transferBucket = new s3.Bucket(this, "TransferBucket", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    new s3deploy.BucketDeployment(this, "CreateHomePrefixMarker", {
      destinationBucket: transferBucket,
      destinationKeyPrefix: HOME_PREFIX,
      sources: [s3deploy.Source.data(".keep", "")],
      retainOnDelete: false,
    });

    // ============================================================
    // 6) Transfer role for S3 access
    // ============================================================
    const transferS3Role = new iam.Role(this, "TransferS3Role", {
      assumedBy: new iam.ServicePrincipal("transfer.amazonaws.com"),
      description: "Role assumed by AWS Transfer Family users to access S3",
    });

    transferS3Role.addToPolicy(
      new iam.PolicyStatement({
        actions: ["s3:GetBucketLocation"],
        resources: [transferBucket.bucketArn],
      })
    );

    transferS3Role.addToPolicy(
      new iam.PolicyStatement({
        actions: ["s3:ListBucket"],
        resources: [transferBucket.bucketArn],
        conditions: {
          StringLike: { "s3:prefix": [HOME_PREFIX, `${HOME_PREFIX}/`, `${HOME_PREFIX}/*`] },
        },
      })
    );

    transferS3Role.addToPolicy(
      new iam.PolicyStatement({
        actions: ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:GetObjectVersion", "s3:DeleteObjectVersion"],
        resources: [transferBucket.arnForObjects(`${HOME_PREFIX}/*`)],
      })
    );

    // ============================================================
    // 7) Identity Provider Lambda (Python) + API Gateway
    //    ✅ FIX: parse serverId/username from the PATH (your event proves that)
    // ============================================================
    const idpFn = new lambda.Function(this, "TransferIdpLambda", {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "index.handler",
      timeout: cdk.Duration.seconds(10),
      memorySize: 256,
      logRetention: logs.RetentionDays.ONE_WEEK,
      environment: { USER_SECRET_PREFIX: "transfer" },
      code: lambda.Code.fromInline(`
import json, os, re, base64, boto3
sm = boto3.client("secretsmanager")

# Expected Transfer path:
# /servers/{serverId}/users/{username}/config
PATH_RE = re.compile(r"/servers/(?P<serverId>[^/]+)/users/(?P<username>[^/]+)/config")

def _json_loads_maybe(s):
    if not s or not isinstance(s, str):
        return {}
    try:
        return json.loads(s)
    except Exception:
        return {}

def handler(event, context):
    print("EVENT_KEYS:", sorted(list(event.keys())))
    path = event.get("path") or ""
    print("PATH:", path)

    # 1) Try to get serverId/username from pathParameters, else parse path
    pp = event.get("pathParameters") or {}
    server_id = pp.get("serverId") or pp.get("ServerId") or pp.get("serverId")
    username = pp.get("username") or pp.get("Username") or pp.get("userName")

    m = PATH_RE.search(path)
    if m:
        server_id = server_id or m.group("serverId")
        username = username or m.group("username")

    # 2) Password may be in body JSON; sometimes body is empty (as you saw)
    body = event.get("body")
    if event.get("isBase64Encoded") and isinstance(body, str):
        try:
            body = base64.b64decode(body).decode("utf-8", errors="ignore")
        except Exception:
            body = ""

    payload = _json_loads_maybe(body)
    print("BODY_LEN:", 0 if body is None else len(body))
    print("BODY_KEYS:", sorted(list(payload.keys())))

    password = (
        payload.get("Password") or payload.get("password") or
        payload.get("UserPassword") or payload.get("userPassword")
    )

    print("USERNAME:", username)
    print("SERVER_ID:", server_id)
    print("PASSWORD_PRESENT:", bool(password))

    prefix = os.environ.get("USER_SECRET_PREFIX", "transfer")

    if not username or not server_id:
        # Transfer treats non-200 as deny; keep it explicit
        return {"statusCode": 403, "body": ""}

    secret_id = f"{prefix}/{server_id}/{username}"

    try:
        resp = sm.get_secret_value(SecretId=secret_id)
        secret = json.loads(resp.get("SecretString") or "{}")
    except Exception as e:
        print("SECRET_LOOKUP_FAILED:", str(e))
        return {"statusCode": 403, "body": ""}

    expected = secret.get("Password")

    # Only reject if password WAS supplied and is wrong
    if isinstance(expected, str) and password is not None and password != expected:
        print("PASSWORD_MISMATCH")
        return {"statusCode": 403, "body": ""}

    role = secret.get("Role")
    home = secret.get("HomeDirectoryDetails")
    if not role or not home:
        print("BAD_SECRET_CONTENTS")
        return {"statusCode": 403, "body": ""}

    out = {
        "Role": role,
        "HomeDirectoryType": "LOGICAL",
        "HomeDirectoryDetails": home,
    }
    return {"statusCode": 200, "body": json.dumps(out)}
      `),
    });

    idpFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
        resources: ["*"],
      })
    );

    const idpApi = new apigw.RestApi(this, "TransferIdpApi", {
      restApiName: "TransferIdpApi",
      deployOptions: { stageName: "prod" },
      endpointTypes: [apigw.EndpointType.REGIONAL],
    });

    // Proxy everything so /servers/{id}/users/{name}/config reaches the lambda
    idpApi.root.addProxy({
      defaultIntegration: new apigw.LambdaIntegration(idpFn, { proxy: true }),
      anyMethod: true,
    });

    const transferInvokeRole = new iam.Role(this, "TransferInvokeRole", {
      assumedBy: new iam.ServicePrincipal("transfer.amazonaws.com"),
    });

    transferInvokeRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["execute-api:Invoke"],
        resources: [idpApi.arnForExecuteApi("*", "/*", "prod")],
      })
    );

    // ============================================================
    // 8) Transfer FTPS Server
    // ============================================================
    const server = new transfer.CfnServer(this, "FtpsServer", {
      protocols: ["FTPS"],
      endpointType: "VPC",
      certificate: cert.certificateArn,
      domain: "S3",
      identityProviderType: "API_GATEWAY",
      identityProviderDetails: {
        url: idpApi.url, // includes /prod/
        invocationRole: transferInvokeRole.roleArn,
      },
      endpointDetails: {
        vpcId: vpc.vpcId,
        subnetIds: [vpc.publicSubnets[0].subnetId],
        securityGroupIds: [ftpsSg.securityGroupId],
        addressAllocationIds: [ftpsEip.attrAllocationId],
      },
      protocolDetails: { tlsSessionResumptionMode: "ENFORCED" },
    });

    // ============================================================
    // 9) Per-user secret: transfer/<serverId>/<username>
    // ============================================================
    const homeDetails = JSON.stringify([{ Entry: "/", Target: `/${transferBucket.bucketName}/${HOME_PREFIX}` }]);
    const transferUserSecretName = `transfer/${server.attrServerId}/${TRANSFER_USERNAME}`;

    const userSecret = new secretsmanager.Secret(this, "TransferUserSecret", {
      secretName: transferUserSecretName,
      secretStringValue: cdk.SecretValue.unsafePlainText(
        JSON.stringify({
          Password: TRANSFER_PASSWORD,
          Role: transferS3Role.roleArn,
          HomeDirectoryType: "LOGICAL",
          HomeDirectoryDetails: homeDetails,
        })
      ),
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    userSecret.node.addDependency(server);

    // ============================================================
    // OUTPUTS (including required networking JSON format)
    // ============================================================
    new cdk.CfnOutput(this, "NetworkingJsonOutput", {
      value: cdk.Stack.of(this).toJsonString({
        networking: {
          vpcId: vpc.vpcId,
          subnetIds: lambdaSubnets.subnetIds,
          securityGroupIds: [lambdaClientSg.securityGroupId],
        },
      }),
    });

    new cdk.CfnOutput(this, "FtpsPublicIpOutput", { value: ftpsEip.ref });
    new cdk.CfnOutput(this, "FtpsHostnameOutput", { value: FTPS_FQDN });
    new cdk.CfnOutput(this, "LambdaNatPublicIpOutput", { value: lambdaNatEip.ref });
    new cdk.CfnOutput(this, "FtpsServerIdOutput", { value: server.attrServerId });
    new cdk.CfnOutput(this, "FtpsServerSecurityGroupIdOutput", { value: ftpsSg.securityGroupId });
    new cdk.CfnOutput(this, "TransferUserSecretNameOutput", { value: transferUserSecretName });
    new cdk.CfnOutput(this, "TransferIdpApiUrlOutput", { value: idpApi.url });
  }
}
