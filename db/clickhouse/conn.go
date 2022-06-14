package ch

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func GetClickhouseConn(dbName string) (error, clickhouse.Conn, context.Context) {
	if dbName == "" {
		dbName = os.Getenv("CH_DATABASE")
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{os.Getenv("CH_HOST") + ":" + os.Getenv("CH_PORT")},
		Auth: clickhouse.Auth{
			Database: dbName,
			Username: os.Getenv("CH_USER"),
			Password: os.Getenv("CH_PASSWORD"),
		},
		DialTimeout:     time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		//Debug:           true,
	})
	if err != nil {
		return err, nil, nil
	}
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"max_block_size": 10,
	}), clickhouse.WithProgress(func(p *clickhouse.Progress) {
		fmt.Println("progress: ", p)
	}), clickhouse.WithProfileInfo(func(p *clickhouse.ProfileInfo) {
		fmt.Println("profile info: ", p)
	}))
	return nil, conn, ctx
}
