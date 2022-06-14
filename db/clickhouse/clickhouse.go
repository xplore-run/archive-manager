package ch

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func GetRowCount(conn clickhouse.Conn, ctx context.Context, dbName string, table string, noOfDays int) (error, uint64) {
	// Fetch number of rows that can be archived
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT count(*) as rowCount FROM %s.%s where created_at < toStartOfDay(toDate(now()-INTERVAL %d DAY))", dbName, table, noOfDays))
	if err != nil {
		return err, 0
	}

	var (
		rowCount uint64
	)

	for rows.Next() {
		if err := rows.Scan(&rowCount); err != nil {
			return err, 0
		}
	}
	rows.Close()

	if rows.Err() != nil {
		return rows.Err(), 0
	}

	return nil, rowCount
}

// Dump required data in csv file
func CopyToCsvFile(dbName string, table string, noOfDays int, csvFilePath string) (error, uint64) {
	cmd := exec.Command("clickhouse-client", "-h", os.Getenv("CH_HOST"), "--port", os.Getenv("CH_PORT"), "-d", dbName, "-u", os.Getenv("CH_USER"), "--password", os.Getenv("CH_PASSWORD"),
		"--query", fmt.Sprintf("(SELECT * FROM %s.%s where created_at < toStartOfDay(toDate(now()-INTERVAL %d DAY)))", dbName, table, noOfDays), "--format", "CSVWithNames")

	// open the out file for writing
	outfile, err := os.Create(csvFilePath)
	if err != nil {
		panic(err)
	}
	defer outfile.Close()
	cmd.Stdout = outfile

	err = cmd.Start()
	if err != nil {
		panic(err)
	}
	cmd.Wait()
	return lineCount(csvFilePath)
}

// Get line count for a given file path
func lineCount(filePath string) (error, uint64) {
	cmd := exec.Command("wc", "-l", filePath)
	stdout, err := cmd.Output()

	if err != nil {
		fmt.Println(err.Error())
		return err, 0
	}
	print(string(stdout))
	strL := strings.Split(string(stdout), filePath)
	strTrim := strings.TrimSpace(strL[0])
	rowCopiedCount, err := strconv.Atoi(strTrim)
	if err != nil {
		fmt.Println(err)
		return err, 0
	}
	return nil, uint64(rowCopiedCount)
}

// Delete rows older than given noOfDays from given table in given schema
func DeleteDataFromTable(conn clickhouse.Conn, ctx context.Context, dbName string, table string, noOfDays int) error {
	err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE  %s.%s DELETE where created_at < toStartOfDay(toDate(now()-INTERVAL %d DAY))", dbName, table, noOfDays))
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
	}
	return err
}
