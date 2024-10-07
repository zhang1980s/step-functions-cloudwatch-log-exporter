#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { StepFunctionsCloudwatchLogExporterStack } from '../lib/step-functions-cloudwatch-log-exporter-stack';

const app = new cdk.App();
const stackname = app.node.tryGetContext('stackName');
new StepFunctionsCloudwatchLogExporterStack(app, 'StepFunctionsCloudwatchLogExporterStack', {
    stackName: stackname,
    env: {
        account: process.env.CDK_DEPLOY_ACCOUNT || process.env.CDK_DEFAULT_ACCOUNT,
        region: process.env.CDK_DEPLOY_REGION || process.env.CDK_DEFAULT_REGION,
    }
});