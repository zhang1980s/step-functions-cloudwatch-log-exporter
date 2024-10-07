import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as sns from 'aws-cdk-lib/aws-sns';
import { Construct } from 'constructs';
import * as path from 'path';

export class StepFunctionsCloudwatchLogExporterStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create SNS Topic for failed exports
    const failedExportsTopic = new sns.Topic(this, 'FailedExportsTopic', {
      topicName: 'cloudwatch-log-export-failures',
    });

    // Create CloudFormation parameters
    const regionBucketMapParameter = new cdk.CfnParameter(this, 'RegionBucketMapParameter', {
      type: 'String',
      description: 'JSON string of region to S3 bucket mapping',
      default: '{"us-east-1":"s3://my-bucket-us-east-1","us-west-2":"s3://my-bucket-us-west-2"}',
    });

    const exportDaysParameter = new cdk.CfnParameter(this, 'ExportDaysParameter', {
      type: 'Number',
      description: 'Number of days of logs to export',
      default: 1,
      minValue: 1,
    });

    const exportlogPrefixParameter = new cdk.CfnParameter(this, 'ExportlogPrefixParameter', {
      type: 'String',
      description: 'Prefix for exported logs in S3',
      default: 'cloudwatch-logs',
    });

    const scheduleParameter = new cdk.CfnParameter(this, 'ScheduleParameter', {
      type: 'String',
      description: 'The cron schedule for the event rule',
      default: 'cron(5 0 * * ? *)', // UTC
    });

    // Create Lambda function
    const exportLambda = new lambda.Function(this, 'ExportLambda', {
      runtime: lambda.Runtime.PROVIDED_AL2023,
      architecture: lambda.Architecture.ARM_64,
      handler: 'bootstrap',
      timeout: cdk.Duration.minutes(10),
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda'), {
        bundling: {
          image: lambda.Runtime.PROVIDED_AL2023.bundlingImage,
          command: [
            'bash',
            '-c',
            'export GOARCH=arm64 GOOS=linux && ' +
            'export GOPATH=/tmp/go && ' +
            'mkdir -p /tmp/go && ' +
            'go build -tags lambda.norpc -o bootstrap && ' +
            'cp bootstrap /asset-output/'
          ],
          user: 'root',
        },
      }),
      environment: {
        REGION_BUCKET_MAP: regionBucketMapParameter.valueAsString,
      },
    });

    // Grant permissions to Lambda
    failedExportsTopic.grantPublish(exportLambda);
    exportLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        'logs:DescribeLogGroups',
        'logs:CreateExportTask',
        'logs:DescribeExportTasks',
        'logs:ListTagsForResource',
      ],
      resources: ['*'],
    }));
    exportLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['s3:PutObject'],
      resources: ['arn:aws:s3:::*/*'],
    }));

    // Define Step Functions tasks
    const listLogGroups = new tasks.LambdaInvoke(this, 'ListLogGroups', {
      lambdaFunction: exportLambda,
      payload: sfn.TaskInput.fromObject({
        action: 'listLogGroups',
        inputParameters: sfn.JsonPath.entirePayload,
      }),
      outputPath: '$.Payload',
    });

    const processLogGroups = new sfn.Map(this, 'ProcessLogGroups', {
      maxConcurrency: 1,
      itemsPath: sfn.JsonPath.stringAt('$'),
    })

    const createExportTask = new tasks.LambdaInvoke(this, 'CreateExportTask', {
      lambdaFunction: exportLambda,
      payload: sfn.TaskInput.fromObject({
        action: 'createExportTask',
        logGroups: sfn.JsonPath.array(sfn.JsonPath.stringAt('$')),
        inputParameters: sfn.JsonPath.entirePayload,
      }),
      outputPath: '$.Payload',
    });

    const checkExportTaskStatus = new tasks.LambdaInvoke(this, 'CheckExportTaskStatus', {
      lambdaFunction: exportLambda,
      payload: sfn.TaskInput.fromObject({
        action: 'checkExportTaskStatus',
        logGroups: sfn.JsonPath.array(sfn.JsonPath.stringAt('$')),
      }),
      outputPath: '$.Payload',
    });

    const notifyFailure = new tasks.LambdaInvoke(this, 'NotifyFailure', {
      lambdaFunction: exportLambda,
      payload: sfn.TaskInput.fromObject({
        action: 'notifyFailure',
        logGroups: sfn.JsonPath.array(sfn.JsonPath.stringAt('$')),
      }),
      outputPath: '$.Payload',
    });

    // Define wait states
    const wait30SecondsForExport = new sfn.Wait(this, 'Wait30SecondsForExport', {
      time: sfn.WaitTime.duration(cdk.Duration.seconds(30)),
    });

    // Define Step Functions workflow
    const processLogGroup = createExportTask
        .next(wait30SecondsForExport)
        .next(checkExportTaskStatus)
        .next(new sfn.Choice(this, 'ExportTaskStatus')
            .when(sfn.Condition.stringEquals('$.taskStatus', 'COMPLETED'), new sfn.Succeed(this, 'ExportSucceeded'))
            .when(sfn.Condition.or(
                sfn.Condition.stringEquals('$.taskStatus', 'CANCELLED'),
                sfn.Condition.stringEquals('$.taskStatus', 'FAILED'),
                sfn.Condition.stringEquals('$.taskStatus', 'PENDING_CANCEL')
            ), notifyFailure)
            .otherwise(wait30SecondsForExport)
        );

    const definition = listLogGroups
        .next(
            processLogGroups
                .itemProcessor(processLogGroup)
        );

    // Create Step Functions state machine
    const stateMachine = new sfn.StateMachine(this, 'ExportStateMachine', {
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      timeout: cdk.Duration.hours(24),
    });

    // Create EventBridge rule to trigger the state machine
    new events.Rule(this, 'DailyExportRule', {
      schedule: events.Schedule.expression(scheduleParameter.valueAsString),
      targets: [new targets.SfnStateMachine(stateMachine, {
        input: events.RuleTargetInput.fromObject('{}'),
      })],
    });
  }
}