package azure

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

func UploadFile(containerName string, filePath string) error {
	connStr := os.Getenv("AZURE_STORAGE_CONN_STR")
	serviceClient, err := azblob.NewServiceClientFromConnectionString(connStr, nil)

	ctx := context.Background() // This example has no expiry.

	containerName = strings.ReplaceAll(containerName, "_", "-")
	// First, branch off of the service client and create a container client.
	container, err := serviceClient.NewContainerClient(containerName)
	_, err = container.Create(ctx, nil)
	handle(err)

	// Get the fileName from Path
	fileName := filepath.Base(filePath)

	// Open the file from the file path
	upFile, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("could not open local filepath [%v]: %+v", filePath, err)
	}
	defer upFile.Close()

	blockBlob, err := container.NewBlockBlobClient(fileName)

	_, err = blockBlob.UploadFile(ctx, upFile, azblob.UploadOption{})
	handle(err)
	return err
}

func handle(err error) {
	print(err)
}
