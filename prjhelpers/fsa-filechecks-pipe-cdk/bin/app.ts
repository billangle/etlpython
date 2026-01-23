#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { FsaFileChecksPipeStack } from '../lib/fsa-filechecks-pipe-stack';

const app = new cdk.App();

new FsaFileChecksPipeStack(app, 'FsaFileChecksPipeStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  }
});
