#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { JenkinsHookStack } from "../lib/jenkins-hook-stack";

const app = new cdk.App();

new JenkinsHookStack(app, "JenkinsHookStack", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION
  }
});
