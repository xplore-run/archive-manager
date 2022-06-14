package main

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"gitlab.com/oyerickshaw/site-reliability/archive-manager/archive"
	ch "gitlab.com/oyerickshaw/site-reliability/archive-manager/db/clickhouse"
	pg "gitlab.com/oyerickshaw/site-reliability/archive-manager/db/postgres"
)

var ColorRed = "\033[31m"
var ColorGreen = "\033[32m"
var ColorYellow = "\033[33m"

func main() {
	// Load env variables from .env in DEV environment
	if os.Getenv("GO_ENV") == "DEV" {
		err := godotenv.Load()

		if err != nil {
			fmt.Println("Error loading .env file", err)
		}
	}

	// parse yaml config file
	data, err := archive.ParseYaml()
	if err != nil {
		log.Fatalf("Error in parsing yaml file")
	}

	//handle archive
	for _, v := range data {
		archiveList := v.Archive_list
		config := v.Config
		archive.ConfigStore.CloudProvider = config.Cloud_provider
		archive.ConfigStore.AwsS3BucketName = config.Aws_s3_bucket_name
		archive.ConfigStore.TimestampColumn = config.Timestamp_column

		for _, v := range archiveList {
			if v.Db == "" {
				println("database type not provided", v.Db)
			} else if v.Db == "postgres" {
				client := pg.GetPgClient(v.Db_name)
				archive.HandleArchivePostgres(client, v)
			} else if v.Db == "clickhouse" {
				err, client, ctx := ch.GetClickhouseConn(v.Db_name)
				if err != nil {
					println("\n Error while getting clickhouse client", err)
					continue
				}
				archive.HandleArchiveClickhouse(client, ctx, v)
			} else {
				println("Unknown database", v.Db)
			}
		}
	}
}
