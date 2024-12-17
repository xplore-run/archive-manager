# archive-x
NOTE - This is in BETA phase.

## Overview
`archive-x` is a Go-based archiving service designed to efficiently manage and archive documents from MongoDB to Google Cloud Storage (GCS). The service supports both partitioned and non-partitioned collections and uses Redis for locking and tracking the last processed document.

## Features
- Archiving documents from MongoDB to GCS
- Supports both hourly partitioned and non-partitioned collections
- Uses Redis for locking and tracking the last processed document
- Customizable via environment variables

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

Running Tests
To run the tests, use the following command:
```sh
go test ./...
```


Example
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
## Contributing
Contributions are welcome! Please open an issue or submit a pull request.
1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

License
This project is licensed under the MIT License.
