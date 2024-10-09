package main

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"go.uber.org/zap"
)

var (
	logger          *zap.SugaredLogger
	cwLogsClient    *cloudwatchlogs.Client
	snsClient       *sns.Client
	regionBucketMap map[string]string
	exportDays      int
	snsTopic        string
	exportlogPrefix string
)

type InputParameters struct {
	RegionBucketMap map[string]string `json:"regionBucketMap"`
	ExportDays      int               `json:"exportDays"`
	SnsTopicArn     string            `json:"snsTopicArn"`
	ExportlogPrefix string            `json:"exportlogPrefix"`
}

type Event struct {
	Action          string          `json:"action"`
	LogGroups       []LogGroup      `json:"logGroups,omitempty"`
	InputParameters InputParameters `json:"inputParameters,omitempty"`
}

type LogGroup struct {
	Region            string    `json:"region"`
	Name              string    `json:"name"`
	S3Destination     string    `json:"s3Destination"`
	From              int64     `json:"from"`
	To                int64     `json:"to"`
	DestinationPrefix string    `json:"destinationPrefix"`
	TaskStatus        string    `json:"taskStatus"`
	TaskId            string    `json:"taskId,omitempty"`
	StartTime         time.Time `json:"startTime,omitempty"`
	EndTime           time.Time `json:"endTime,omitempty"`
	NotificationArn   string    `json:"notificationArn"`
}

const (
	maxRetries = 3
	baseDelay  = 100 * time.Millisecond
)

func init() {
	zapLogger, err := zap.NewProduction()
	if err != nil {
		panic("Failed to initialize logger: " + err.Error())
	}
	logger = zapLogger.Sugar()

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		logger.Fatalf("Failed to load AWS config: %v", err)
	}

	cwLogsClient = cloudwatchlogs.NewFromConfig(cfg)
	snsClient = sns.NewFromConfig(cfg)
}

func main() {
	lambda.Start(HandleRequest)
}

func HandleRequest(ctx context.Context, event Event) (interface{}, error) {
	logger.Infow("Received event", "action", event.Action)

	regionBucketMap = event.InputParameters.RegionBucketMap
	exportDays = event.InputParameters.ExportDays
	snsTopic = event.InputParameters.SnsTopicArn
	exportlogPrefix = event.InputParameters.ExportlogPrefix

	switch event.Action {
	case "listLogGroups":
		return listLogGroups(ctx)
	case "createExportTask":
		if len(event.LogGroups) == 0 {
			logger.Error("No log group provided for createExportTask")
			return nil, errors.New("no log group provided for createExportTask")
		}
		return createExportTask(ctx, &event.LogGroups[0])
	case "checkExportTaskStatus":
		if len(event.LogGroups) == 0 {
			logger.Error("No log group provided for checkExportTaskStatus")
			return nil, errors.New("no log group provided for checkExportTaskStatus")
		}
		return checkExportTaskStatus(ctx, &event.LogGroups[0])
	case "notifyFailure":
		if len(event.LogGroups) == 0 {
			logger.Error("No log group provided for notifyFailure")
			return nil, errors.New("no log group provided for notifyFailure")
		}
		err := notifyFailure(ctx, event.LogGroups[0])
		if err != nil {
			return nil, err
		}
		return map[string]bool{"notified": true}, nil
	default:
		logger.Errorw("Unknown action", "action", event.Action)
		return nil, errors.New("unknown action: " + event.Action)
	}
}

func listLogGroups(ctx context.Context) ([]LogGroup, error) {
	logger.Info("Starting listLogGroups function")
	var logGroups []LogGroup
	now := time.Now().UTC()
	from := now.AddDate(0, 0, -exportDays).Truncate(24 * time.Hour)
	to := now.Truncate(24 * time.Hour)

	for region, bucket := range regionBucketMap {
		logger.Infow("Processing region", "region", region, "bucket", bucket)
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			logger.Errorw("Error loading config for region", "region", region, "error", err)
			continue
		}

		cwLogsClient := cloudwatchlogs.NewFromConfig(cfg)
		paginator := cloudwatchlogs.NewDescribeLogGroupsPaginator(cwLogsClient, &cloudwatchlogs.DescribeLogGroupsInput{})
		pageCount := 0
		for paginator.HasMorePages() {
			pageCount++
			logger.Infow("Processing page", "page", pageCount, "region", region)
			var page *cloudwatchlogs.DescribeLogGroupsOutput
			var err error
			err = retryWithExponentialBackoff(func() error {
				page, err = paginator.NextPage(ctx)
				return err
			})
			if err != nil {
				logger.Errorw("Error listing log groups", "region", region, "error", err)
				continue
			}

			logger.Infow("Found log groups", "count", len(page.LogGroups), "page", pageCount, "region", region)
			for _, logGroup := range page.LogGroups {
				taskStatus := "PENDING"
				logGroupName := aws.ToString(logGroup.LogGroupName)
				logger.Infow("Processing log group", "name", logGroupName)
				logGroupArn := "arn:aws:logs:" + region + ":" + getAccountID(ctx) + ":log-group:" + logGroupName
				var tags *cloudwatchlogs.ListTagsForResourceOutput
				err = retryWithExponentialBackoff(func() error {
					tags, err = cwLogsClient.ListTagsForResource(ctx, &cloudwatchlogs.ListTagsForResourceInput{
						ResourceArn: aws.String(logGroupArn),
					})
					return err
				})
				if err != nil {
					logger.Errorw("Error listing tags for log group", "name", logGroupName, "error", err)
				} else {
					logger.Infow("Tags for log group", "name", logGroupName, "tags", tags.Tags)
					if value, exists := tags.Tags["auto-export"]; exists && value == "no" {
						taskStatus = "SKIP"
						logger.Infow("Log group marked for SKIP", "name", logGroupName)
					}
				}

				destinationPrefix := exportlogPrefix + logGroupName + "/year=" +
					from.Format("2006") + "/month=" + from.Format("01") +
					"/day=" + from.Format("02")

				logGroups = append(logGroups, LogGroup{
					Region:            region,
					Name:              logGroupName,
					TaskStatus:        taskStatus,
					S3Destination:     strings.TrimPrefix(bucket, "s3://"),
					From:              from.Unix() * 1000,
					To:                to.Unix() * 1000,
					DestinationPrefix: destinationPrefix,
					NotificationArn:   snsTopic,
				})
				logger.Infow("Added log group", "name", logGroupName, "status", taskStatus, "s3Destination", bucket, "destinationPrefix", destinationPrefix)
			}
		}
		logger.Infow("Finished processing region", "region", region, "logGroupsCount", len(logGroups))
	}
	logger.Infow("listLogGroups function completed", "totalLogGroups", len(logGroups))
	return logGroups, nil
}

func createExportTask(ctx context.Context, logGroup *LogGroup) (*LogGroup, error) {
	logger.Infow("Starting createExportTask", "logGroup", logGroup.Name, "region", logGroup.Region)
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(logGroup.Region))
	if err != nil {
		logger.Errorw("Error loading config for region", "region", logGroup.Region, "error", err)
		return nil, errors.New("error loading config for region: " + err.Error())
	}

	cwLogsClient := cloudwatchlogs.NewFromConfig(cfg)
	input := &cloudwatchlogs.CreateExportTaskInput{
		Destination:       aws.String(logGroup.S3Destination),
		LogGroupName:      aws.String(logGroup.Name),
		From:              aws.Int64(logGroup.From),
		To:                aws.Int64(logGroup.To),
		DestinationPrefix: aws.String(logGroup.DestinationPrefix),
	}

	var output *cloudwatchlogs.CreateExportTaskOutput
	err = retryWithExponentialBackoff(func() error {
		var err error
		output, err = cwLogsClient.CreateExportTask(ctx, input)
		return err
	})
	if err != nil {
		logger.Errorw("Error creating export task", "logGroup", logGroup.Name, "error", err)
		return nil, errors.New("error creating export task: " + err.Error())
	}

	logGroup.TaskId = *output.TaskId
	logger.Infow("Export task created", "logGroup", logGroup.Name, "taskId", logGroup.TaskId)
	return logGroup, nil
}

func checkExportTaskStatus(ctx context.Context, logGroup *LogGroup) (*LogGroup, error) {
	logger.Infow("Checking export task status", "logGroup", logGroup.Name, "taskId", logGroup.TaskId)
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(logGroup.Region))
	if err != nil {
		logger.Errorw("Error loading config for region", "region", logGroup.Region, "error", err)
		return nil, errors.New("error loading config for region: " + err.Error())
	}

	cwLogsClient := cloudwatchlogs.NewFromConfig(cfg)
	input := &cloudwatchlogs.DescribeExportTasksInput{
		TaskId: aws.String(logGroup.TaskId),
	}

	var output *cloudwatchlogs.DescribeExportTasksOutput
	err = retryWithExponentialBackoff(func() error {
		var err error
		output, err = cwLogsClient.DescribeExportTasks(ctx, input)
		return err
	})
	if err != nil {
		logger.Errorw("Error describing export task", "taskId", logGroup.TaskId, "error", err)
		return nil, errors.New("error describing export task: " + err.Error())
	}

	if len(output.ExportTasks) == 0 {
		logger.Errorw("Export task not found", "taskId", logGroup.TaskId)
		return nil, errors.New("export task not found")
	}

	task := output.ExportTasks[0]
	logGroup.StartTime = time.Unix(0, *task.From*int64(time.Millisecond))
	logGroup.EndTime = time.Unix(0, *task.To*int64(time.Millisecond))
	logGroup.TaskStatus = string(task.Status.Code)

	logger.Infow("Export task status updated", "logGroup", logGroup.Name, "taskId", logGroup.TaskId, "status", logGroup.TaskStatus)
	return logGroup, nil
}

func notifyFailure(ctx context.Context, logGroup LogGroup) error {
	logger.Infow("Notifying failure", "logGroup", logGroup.Name, "taskId", logGroup.TaskId)

	message := "Export task failed for log group " + logGroup.Name +
		" in region " + logGroup.Region +
		". Task ID: " + logGroup.TaskId +
		", Status: " + logGroup.TaskStatus +
		", Start Time: " + logGroup.StartTime.Format(time.RFC3339)

	input := &sns.PublishInput{
		Message:  aws.String(message),
		TopicArn: aws.String(logGroup.NotificationArn),
	}

	err := retryWithExponentialBackoff(func() error {
		_, err := snsClient.Publish(ctx, input)
		return err
	})
	if err != nil {
		logger.Errorw("Error publishing to SNS", "error", err, "logGroup", logGroup.Name, "taskId", logGroup.TaskId)
		return errors.New("error publishing to SNS: " + err.Error())
	}

	logger.Info("Failure notification sent successfully")
	return nil
}

func getAccountID(ctx context.Context) string {
	lambdaContext, ok := lambdacontext.FromContext(ctx)
	if !ok {
		logger.Warn("Could not retrieve Lambda context")
		return ""
	}

	arnParts := strings.Split(lambdaContext.InvokedFunctionArn, ":")
	if len(arnParts) >= 5 {
		return arnParts[4]
	}

	logger.Warn("Could not extract account ID from Lambda context")
	return ""
}

func retryWithExponentialBackoff(operation func() error) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		err = operation()
		if err == nil {
			return nil
		}
		if i == maxRetries-1 {
			break
		}
		delay := time.Duration(math.Pow(2, float64(i))) * baseDelay
		logger.Infow("Retrying operation", "attempt", i+1, "delay", delay)
		time.Sleep(delay)
	}
	return err
}
