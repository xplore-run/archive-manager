# archive-x

**NOTE**: This is in BETA phase.

## Overview

`archive-x` is a Go-based archiving service designed to efficiently manage and archive documents from MongoDB to Google Cloud Storage (GCS). The service supports both partitioned and non-partitioned collections and uses Redis for locking and tracking the last processed document.

## Features

- Archiving documents from MongoDB to GCS
- Supports both hourly partitioned and non-partitioned collections
- Uses Redis for locking and tracking the last processed document
- Customizable via environment variables

## Supported Databae
- Mongodb
- Will be adding more soon.

## Supported Cloud Provider
- Google Cloud Storage
- Will be adding more soon.

## Installation

```bash
go get github.com/xplore-run/archive-manager
```

## Environment Variables

The following environment variables are used to configure the archiver:

- `PARTITION_PREFIX`: Prefix for partitioned collections
- `WRITER_COLLECTION_TYPE`: Type of writer collection (`hourly_partition` or `no_partition`)
- `APP_DIR`: Directory for application logs and temporary files
- `DEFAULT_MONGODB_URL`: MongoDB connection URL
- `DEFAULT_MOGO_DB`: Default MongoDB database name
- `GCP_BUCKET_NAME`: Google Cloud Storage bucket name
- `UPTO`: Duration for archiving documents (e.g., `24h`, `1d`, `1w`, `1m`)
- `ARCHIVER_NAME`: Name of the archiver
- `MONGO_COLLECTION_NAME`: Name of the MongoDB collection
- `REDIS_LAST_ID_KEY`: Redis key for tracking the last processed document ID
- `REDIS_LOCK_KEY`: Redis key for locking
- `REDIS_PROCESSED_KEY`: Redis key for tracking processed collections
- `MAX_ID`: Maximum document ID to process

## Usage

### Running the Archiver

To run the archiver, ensure all required environment variables are set and execute the following command:

```sh
go run main.go
```

### Running Tests

To run the tests, use the following command:

```sh
go test ./...
```

## Example Configuration

Here is an example of setting environment variables and running the archiver:

```sh
export PARTITION_PREFIX="test_prefix"
export WRITER_COLLECTION_TYPE="no_partition"
export APP_DIR="/tmp"
export DEFAULT_MONGODB_URL="mongodb://localhost:27017"
export DEFAULT_MOGO_DB="test_db"
export GCP_BUCKET_NAME="test_bucket"
export UPTO="24h"
export ARCHIVER_NAME="test_archiver"
export MONGO_COLLECTION_NAME="test_collection"
export REDIS_LAST_ID_KEY="test_last_id_key"
export REDIS_LOCK_KEY="test_lock_key"
export REDIS_PROCESSED_KEY="test_processed_key"
export MAX_ID=""

go run main.go
```

## Example Code

```go
package main

import (
    "encoding/csv"
    "fmt"
    "os"
    "time"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/bson/primitive"

    "github.com/xplore-run/archive-manager/pkg/archiver"
)

// Log struct
type Log struct {
    ID         primitive.ObjectID `bson:"_id" json:"_id"`
    Payload    string             `bson:"payload" json:"payload"`
    Message    string             `bson:"message" json:"message"`
    HttpStatus int                `bson:"http_status" json:"http_status"`
    CreatedAt  time.Time          `bson:"created" json:"created"`
}

// LogWriter struct
type LogWriter struct {
}

// WriteDocumentsToCSV writes the documents to a CSV file
func (w *LogWriter) WriteDocumentsToCSV(filePath string, docs []interface{}) error {
    file, err := os.Create(filePath)
    if err != nil {
        return fmt.Errorf("failed to create CSV file: %v", err)
    }
    defer file.Close()

    writer := csv.NewWriter(file)
    defer writer.Flush()

    for _, doc := range docs {
        docBytes, err := bson.Marshal(doc)
        if err != nil {
            return fmt.Errorf("failed to marshal apiLog: %v", err)
        }
        d := Log{}
        if err := bson.Unmarshal(docBytes, &d); err != nil {
            return fmt.Errorf("failed to unmarshal apiLog: %v", err)
        }

        // Todo: add other/all fields
        writer.Write([]string{
            d.ID.Hex(),
            d.Payload,
            d.Message,
            fmt.Sprintf("%d", d.HttpStatus),
            d.CreatedAt.Format(time.RFC3339),
        })
    }

    return nil
}

// ExtraFilter returns the extra filter to be applied to the query
func (w *LogWriter) ExtraFilter() (bson.M, error) {
    ORG_ID := os.Getenv("ORG_ID")
    if ORG_ID != "" {
        orgId, err := primitive.ObjectIDFromHex(ORG_ID)
        if err != nil {
            return nil, fmt.Errorf("failed to convert org_id to object id: %v", err)
        }

        return bson.M{"org_id": orgId}, nil
    }
    return nil, nil
}

// main function init writer and start archiver
func main() {
    st := archiver.Archiver{Name: archiver.ArchiverName, Writer: &LogWriter{}}

    // Start the archiver
    st.StartArchiver()
}
```

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License.