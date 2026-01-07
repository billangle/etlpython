#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { DartSftpStack } from "../lib/dart-sftp-stack";

const app = new cdk.App();

new DartSftpStack(app, "DartSftpCertStack", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION
  }
});
