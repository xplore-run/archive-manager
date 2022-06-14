package aws

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func UploadFile(filePath string) error {
	awsRegion := os.Getenv("AWS_REGION")
	bucketName := os.Getenv("AWS_BUCKET_NAME")

	// Create an AWS Session. This will use credentials defined in the environment
	session, err := session.NewSession(&aws.Config{Region: aws.String(awsRegion)})
	if err != nil {
		log.Fatalf("could not initialize new aws session: %v", err)
	}

	// Initialize an s3 client from the session created
	s3Client := s3.New(session)

	// Call the upload to s3 file
	err = uploadFileToS3(s3Client, bucketName, filePath)
	if err != nil {
		log.Fatalf("could not upload file: %v", err)
	}

	return err
}

// Uploads a file to AWS S3 given an S3 session client, a bucket name and a file path
func uploadFileToS3(s3Client *s3.S3, bucketName string, filePath string) error {

	// Get the fileName from Path
	fileName := filepath.Base(filePath)

	// Open the file from the file path
	upFile, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("could not open local filepath [%v]: %+v", filePath, err)
	}
	defer upFile.Close()

	// Get the file info
	upFileInfo, _ := upFile.Stat()
	var fileSize int64 = upFileInfo.Size()
	fileBuffer := make([]byte, fileSize)
	upFile.Read(fileBuffer)

	// Put the file object to s3 with the file name
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket:             aws.String(bucketName),
		Key:                aws.String(fileName),
		ACL:                aws.String("private"),
		Body:               bytes.NewReader(fileBuffer),
		ContentLength:      aws.Int64(fileSize),
		ContentType:        aws.String(http.DetectContentType(fileBuffer)),
		ContentDisposition: aws.String("attachment"),
		// ServerSideEncryption: aws.String("AES256"),
	})
	return err
}
