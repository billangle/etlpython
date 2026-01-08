#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { FtpsTransferStack } from "../lib/ftps-transfer-stack";

const app = new cdk.App();

new FtpsTransferStack(app, "DartFtpsCertStack", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || "us-east-1"
  }
});
