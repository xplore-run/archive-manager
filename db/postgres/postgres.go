package pg

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v4"
)

// Get row count
func GetRowCount(client *pgx.Conn, schemaName string, table string, noOfDays int) (error, uint64) {
	var rowCount uint64

	// Fetch number of rows that can be archived
	err := client.QueryRow(context.Background(), fmt.Sprintf("SELECT count(*) FROM %s.%s where created_at < now()-INTERVAL '%d DAY'", schemaName, table, noOfDays)).Scan(&rowCount)
	if err != nil {
		return err, 0
	}
	return nil, rowCount
}

// Dump required data in csv file
func CopyToCsvFile(dbName string, schemaName string, table string, noOfDays int, csvFilePath string) (error, uint64) {
	cmd := exec.Command("psql", "-h", fmt.Sprintf("%s", os.Getenv("PG_HOST")), "-d", fmt.Sprintf("%s", dbName), "-U", fmt.Sprintf("%s", os.Getenv("PG_USER")), "-W", "-p", fmt.Sprintf("%s", os.Getenv("PG_PORT")), "-c",
		fmt.Sprintf("\\copy (SELECT * FROM %s.%s where created_at < now()-INTERVAL '%d DAY') TO '%s' with csv header;", schemaName, table, noOfDays, csvFilePath), "-w")
	stdout, err := cmd.Output()

	if err != nil {
		fmt.Println(err.Error())
		return err, 0
	}

	strL := strings.Split(string(stdout), "COPY ")
	strL2 := strings.Split(strL[1], "\n")
	rowCopiedCount, err := strconv.ParseUint(strL2[0], 10, 64)
	if err != nil {
		fmt.Println(err)
	}
	return nil, rowCopiedCount
}

// Delete rows older than given noOfDays from given table in given schema
func DeleteDataFromTable(client *pgx.Conn, rowCount uint64, schemaName string, table string, noOfDays int) error {
	d, err := client.Exec(context.Background(), fmt.Sprintf("DELETE FROM %s.%s where created_at < now()-INTERVAL '%d DAY'", schemaName, table, noOfDays))
	if err != nil {
		return err
	}
	strL := strings.Split(d.String(), "DELETE ")
	deletedRowCount, err := strconv.ParseUint(strL[1], 10, 64)
	if err != nil {
		return err
	}
	// Verify Delete
	if rowCount == deletedRowCount {
		return nil
	}
	return fmt.Errorf("Delete of required rows is not Verified. Verify manually.")
}
