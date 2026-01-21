import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as apigw from "aws-cdk-lib/aws-apigateway";
import * as path from "path";

export class JenkinsHookStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const table = new dynamodb.Table(this, "JenkinsHookTable", {
      tableName: "JenkinsHookTbale",
      partitionKey: { name: "id", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    const fn = new lambda.Function(this, "JenkinsHookFn", {
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "..", "lambda", "jenkins-hook")),
      environment: {
        TABLE_NAME: table.tableName
      },
      timeout: cdk.Duration.seconds(10),
      memorySize: 256
    });

    table.grantWriteData(fn);

    const api = new apigw.RestApi(this, "JenkinsHookApi", {
      restApiName: "JenkinsHookApi",
      deployOptions: { stageName: "prod" }
    });

    const hook = api.root.addResource("jenkins-hook");
    hook.addMethod("POST", new apigw.LambdaIntegration(fn));

    new cdk.CfnOutput(this, "JenkinsHookUrl", {
      value: `${api.url}jenkins-hook`
    });

    new cdk.CfnOutput(this, "DynamoTableName", {
      value: table.tableName
    });
  }
}
