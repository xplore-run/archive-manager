package gcp

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

type ContentType string

var (
	CONTENT_TYPE_CSV ContentType = "text/csv"
)

// downloadFileFromGCP downloads an object from Google Cloud Storage
func DownloadFileFromGCP(bucketName, objectName, destinationFilePath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
	defer cancel()

	client, err := storage.NewClient(ctx, option.WithCredentialsFile(os.Getenv("GCP_CRED_PATH")))
	if err != nil {
		return fmt.Errorf("[downloadFileFromGCP] failed to create GCP client: %v", err)
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)
	object := bucket.Object(objectName)

	reader, err := object.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("[downloadFileFromGCP] failed to create object reader: %v", err)
	}
	defer reader.Close()

	f, err := os.Create(destinationFilePath)
	if err != nil {
		return fmt.Errorf("[downloadFileFromGCP] failed to create file: %v", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, reader); err != nil {
		return fmt.Errorf("[downloadFileFromGCP] failed to copy object data to file: %v", err)
	}

	fmt.Printf("[downloadFileFromGCP] File downloaded to %s\n", destinationFilePath)
	return nil
}

// UploadFileToGCS uploads the given CSV file to the specified Google Cloud Storage bucket
func UploadFileToGCS(ctx context.Context, bucketName string, objectName string, filePath string, ct ContentType) error {
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(os.Getenv("GCP_CRED_PATH")))
	if err != nil {
		return fmt.Errorf("[UploadFileToGCS] failed to create GCP client: %v", err)
	}
	defer client.Close()

	// Open the CSV file to upload
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	// Get a reference to the bucket and the object (file in GCS)
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(objectName)

	// Upload the file to GCS
	w := obj.NewWriter(ctx)
	w.ContentType = string(ct) // Set the content type of the object

	if _, err = w.Write([]byte{}); err != nil {
		return fmt.Errorf("failed to initiate GCS writer: %v", err)
	}

	// Copy file content to GCS
	if _, err = file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek file: %v", err)
	}

	if _, err = io.Copy(w, file); err != nil {
		return fmt.Errorf("failed to copy file to GCS: %v", err)
	}

	// Close the GCS writer
	if err = w.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer: %v", err)
	}

	// File uploaded successfully
	fmt.Printf("File %s uploaded to GCS bucket %s as object %s\n", filePath, bucketName, objectName)
	return nil
}
