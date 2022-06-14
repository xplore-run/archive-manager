package archive

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"gitlab.com/oyerickshaw/site-reliability/archive-manager/cloud/aws"
	"gitlab.com/oyerickshaw/site-reliability/archive-manager/cloud/azure"
	ch "gitlab.com/oyerickshaw/site-reliability/archive-manager/db/clickhouse"
	pg "gitlab.com/oyerickshaw/site-reliability/archive-manager/db/postgres"
)

func HandleArchivePostgres(client *pgx.Conn, aE ArchiveElement) {
	dbName := aE.Db_name
	schemaName := aE.Schema_name
	tables := aE.Tables
	noOfDays := aE.No_of_days
	cloudProvider := aE.Cloud_provider

	// Validations
	if cloudProvider == "" {
		if ConfigStore.CloudProvider == "" {
			println("\n Error: No cloud provider provided. Provide element level or global level cloud provider in yaml config file")
			return
		} else {
			cloudProvider = ConfigStore.CloudProvider
		}
	}

	defer client.Close(context.Background())

	// Archive each table
	for _, table := range tables {

		//STEP1: Get rowCount of data that can be archived
		err, rowCount := pg.GetRowCount(client, schemaName, table, noOfDays)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\n STEP1: FAILED  Query to  Fetch number of rows that can be archived Failed. %v\n", err)
			continue
		}
		fmt.Printf("\n STEP1: COMPLETED - Number of rows that can be archived : %d", rowCount)

		if rowCount > 0 {
			id := uuid.New()
			//tempCsvFileName format  tableName + date + uniqeId
			tempCsvFilePath := fmt.Sprintf("%spg_%s_d_%s_u_%s.csv", os.Getenv("TEMP_CSV_DUMP_PATH"), table, time.Now().Format("2006-01-02"), id.String())

			//STEP2: Dump data in csv file
			err, rowCopiedCount := pg.CopyToCsvFile(dbName, schemaName, table, noOfDays, tempCsvFilePath)
			if err != nil {
				println("\n STEP2: FAILED - Unable to copy data to csv file:", err)
				continue
			}
			fmt.Printf("\n  STEP2: Number of rows copied in csv file : %d", rowCopiedCount)

			if rowCopiedCount >= rowCount {
				println("\n STEP2: COMPLETED - Verfied: CSV DUMP")

				//STEP3: Upload data to cloud
				if cloudProvider == "azure" {
					err = azure.UploadFile(fmt.Sprintf("%s_%s", schemaName, table), tempCsvFilePath)
					if err != nil {
						println("\n STEP3: FAILED - Unable to upload file to cloud.. Aborting Further steps. Will NOT DELETE data")
						continue
					}
					println("\n STEP3: COMPLETED - File uploaded to cloud")
				} else if cloudProvider == "aws" {
					err := aws.UploadFile(tempCsvFilePath)
					if err != nil {
						println("\n STEP3: FAILED - Unable to upload file to cloud.. Aborting Further steps. Will NOT DELETE data")
						continue
					}
					println("\n STEP3: COMPLETED - File uploaded to cloud")
				} else {
					println("\n STEP3: FAILED - Unknown cloud provider %s", cloudProvider)
				}

				// STEP4: Delete data
				err = pg.DeleteDataFromTable(client, rowCount, schemaName, table, noOfDays)
				if err != nil {
					println("\n STEP4: FAILED -", err)
					continue
				}
				println("\n STEP4: COMPLETED - Rows archived")

				// STEP5: Cleanup
				err := deleteFile(tempCsvFilePath)
				if err != nil {
					println("\n STEP5: FAILED - Cleanup : error while deleting csv file", err)
				}
				println("\n STEP5: COMPLETED - Cleanup")

				println("\n SUCCESS: Archival complete")

			} else {
				fmt.Printf("\n STEP2: FAILED - Number of rows copied in csv file : %d is less than Number of rows that can be archived : %d", rowCopiedCount, rowCount)
				continue
			}
		}
	}
}

func HandleArchiveClickhouse(client clickhouse.Conn, ctx context.Context, aE ArchiveElement) {
	dbName := aE.Db_name
	tables := aE.Tables
	noOfDays := aE.No_of_days
	cloudProvider := aE.Cloud_provider

	if cloudProvider == "" {
		if ConfigStore.CloudProvider == "" {
			println("\n Error: No cloud provider provided. Provide element level or global level cloud provider in yaml config file")
			return
		} else {
			cloudProvider = ConfigStore.CloudProvider
		}
	}

	// Archive each table
	for _, table := range tables {

		//STEP1: Get rowCount of data that can be archived
		err, rowCount := ch.GetRowCount(client, ctx, dbName, table, noOfDays)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\n STEP1: FAILED  Query to  Fetch number of rows that can be archived Failed. %v\n", err)
			continue
		}

		fmt.Printf("\n STEP1: COMPLETED - Number of rows that can be archived : %d", rowCount)

		if rowCount > 0 {
			id := uuid.New()
			//tempCsvFileName format  tableName + date + uniqeId
			tempCsvFilePath := fmt.Sprintf("%sch_%s_d_%s_u_%s.csv", os.Getenv("TEMP_CSV_DUMP_PATH"), table, time.Now().Format("2006-01-02"), id.String())

			//STEP2: Dump data in csv file
			err, rowCopiedCount := ch.CopyToCsvFile(dbName, table, noOfDays, tempCsvFilePath)
			if err != nil {
				println("\n STEP2: FAILED - Unable to copy data to csv file:", err)
				continue
			}
			fmt.Printf("\n  STEP2: Number of rows copied in csv file : %d", rowCopiedCount)

			if rowCopiedCount >= rowCount {
				println("\n STEP2: COMPLETED - Verfied: CSV DUMP")

				//STEP3: Upload data to cloud
				if cloudProvider == "azure" {
					err = azure.UploadFile(fmt.Sprintf("%s_%s", dbName, table), tempCsvFilePath)
					if err != nil {
						println("\n STEP3: FAILED - Unable to upload file to cloud.. Aborting Further steps. Will NOT DELETE data")
						continue
					}
					println("\n STEP3: COMPLETED - File uploaded to cloud")
				} else if cloudProvider == "aws" {
					err := aws.UploadFile(tempCsvFilePath)
					if err != nil {
						println("\n STEP3: FAILED - Unable to upload file to cloud.. Aborting Further steps. Will NOT DELETE data")
						continue
					}
					println("\n STEP3: COMPLETED - File uploaded to cloud")
				} else {
					println("\n STEP3: FAILED - Unknown cloud provider %s", cloudProvider)
					continue
				}

				// STEP 4: Cleanup
				err = deleteFile(tempCsvFilePath)
				if err != nil {
					println("\n STEP 4:CSV File Cleanup - FAILED : error while deleting csv file", err)
					continue
				}
				println("\n STEP 4:CSV File Cleanup - COMPLETED")

				// STEP 5: Delete data, delete is a async task, it will not be instantaneous.
				err = ch.DeleteDataFromTable(client, ctx, dbName, table, noOfDays)
				if err != nil {
					println("\n STEP 5: FAILED -", err)
					continue
				}
				println("\n STEP 5: Rows archived - COMPLETED NOT VERIFIED")

				// wait for decent amount of time(2min) for async delete to finish
				time.Sleep(2 * time.Minute)

				err, rowCount = ch.GetRowCount(client, ctx, dbName, table, noOfDays)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Query to  Fetch number of rows that can be archived Failed. %v\n", err)
					continue
				}

				if rowCount == 0 {
					println("\n STEP 5: Rows archived - COMPLETED VERIFIED")
					println("\n SUCCESS: Archival complete")
				} else {
					println("\n STEP 5: Rows archived - FAILED VERIFICATION FAIL")
					continue
				}

			} else {
				fmt.Printf("\n STEP2: FAILED - Number of rows copied in csv file : %d is less than Number of rows that can be archived : %d", rowCopiedCount, rowCount)
				continue
			}
		}
	}
}

func deleteFile(filePath string) error {
	err := os.Remove(filePath)
	if err != nil {
		return err
	}
	return nil
}
