package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/credentials"
	stds3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pivotal-cf/cf-redis-broker/s3"
	"github.com/pivotal-golang/lager"
)

func main() {
	method := flag.String("method", "cli", "Upload method. [cli|sdk]")
	sourcePath := flag.String("source", "", "Source path.")
	targetPath := flag.String("target", "", "Target path.")
	bucketName := flag.String("bucket", "", "Bucket name.")
	endpoint := flag.String("endpoint", "", "S3 endpoint URL.")
	region := flag.String("region", "", "S3 region.")

	flag.Parse()

	if *sourcePath == "" ||
		*targetPath == "" ||
		*bucketName == "" ||
		*endpoint == "" ||
		*region == "" {
		flag.Usage()
		os.Exit(1)
	}

	logger := lager.NewLogger("s3-upload")
	logger.RegisterSink(lager.NewWriterSink(os.Stdout, lager.DEBUG))

	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	if accessKey == "" {
		logError("getenv", errors.New("Env var AWS_ACCESS_KEY_ID not set"), logger)
	}

	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if secretKey == "" {
		logError("getenv", errors.New("Env var AWS_SECRET_ACCESS_KEY not set"), logger)
	}

	var upload func(string, string, string, string, string, string, string, lager.Logger)

	switch *method {
	case "cli":
		upload = cliUpload
	case "sdk":
		upload = sdkUpload
	default:
		logError("method", errors.New("Unknow upload method."), logger)
	}

	upload(
		*sourcePath,
		*targetPath,
		*bucketName,
		*endpoint,
		*region,
		accessKey,
		secretKey,
		logger,
	)
}

func sdkUpload(sourcePath, targetPath, bucketName, endpoint, region, accessKey, secretKey string, logger lager.Logger) {
	logger.Info("sdk-upload", lager.Data{"event": "starting"})

	config := aws.DefaultConfig
	config.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, "")
	config.Endpoint = endpoint
	config.Region = region

	s3 := stds3.New(config)

	logger.Info("find-bucket", lager.Data{"event": "starting"})

	found, err := bucketExists(s3, bucketName)
	if err != nil {
		logError("find-bucket", err, logger)
	}

	if found {
		logger.Info("find-bucket", lager.Data{"event": "found"})
	} else {
		logger.Info("find-bucket", lager.Data{"event": "not-found"})
	}

	logger.Info("find-bucket", lager.Data{"event": "done"})

	file, err := os.Open(sourcePath)
	if err != nil {
		logError("open-file", err, logger)
	}

	input := &s3manager.UploadInput{
		ACL:    aws.String("private"),
		Bucket: aws.String(bucketName),
		Body:   file,
		Key:    aws.String(targetPath),
	}

	u := s3manager.NewUploader(s3manager.DefaultUploadOptions)

	logger.Info("upload", lager.Data{"event": "starting"})

	if _, err = u.Upload(input); err != nil {
		logError("upload", err, logger)
	}

	logger.Info("upload", lager.Data{"event": "done"})

	logger.Info("sdk-upload", lager.Data{"event": "done"})
}

func bucketExists(svc *stds3.S3, bucketName string) (bool, error) {
	params := &stds3.HeadBucketInput{
		Bucket: aws.String(bucketName), // Required
	}

	_, err := svc.HeadBucket(params)
	if err != nil {
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			if reqErr.StatusCode() == 404 {
				return false, nil
			}
		}
		return false, err
	}

	return true, nil
}

func createBucket(svc *stds3.S3, bucketName string) error {
	params := &stds3.CreateBucketInput{
		Bucket: aws.String(bucketName),
		ACL:    aws.String("private"),
		CreateBucketConfiguration: &stds3.CreateBucketConfiguration{
			LocationConstraint: nil,
		},
		GrantFullControl: aws.String("GrantFullControl"),
		GrantRead:        aws.String("GrantRead"),
		GrantReadACP:     aws.String("GrantReadACP"),
		GrantWrite:       aws.String("GrantWrite"),
		GrantWriteACP:    aws.String("GrantWriteACP"),
	}

	resp, err := svc.CreateBucket(params)
	if err != nil {
		return err
	}

	fmt.Println(awsutil.StringValue(resp))

	return nil
}

func cliUpload(sourcePath, targetPath, bucketName, endpoint, region, accessKey, secretKey string, logger lager.Logger) {
	client := s3.NewClient(
		endpoint,
		accessKey,
		secretKey,
		logger,
	)

	logger.Info("get-bucket", lager.Data{"event": "starting"})

	bucket, err := client.GetOrCreateBucket(bucketName)
	if err != nil {
		logError("get-bucket", err, logger)
	}

	logger.Info("get-bucket", lager.Data{"event": "done"})

	logger.Info("upload", lager.Data{"event": "starting"})

	if err := bucket.Upload(sourcePath, targetPath); err != nil {
		logError("upload", err, logger)
	}

	logger.Info("upload", lager.Data{"event": "done"})
}

func logError(action string, err error, logger lager.Logger) {
	logger.Error(action, err)
	fmt.Println(err.Error())
	os.Exit(1)
}
