package pg

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4"
)

func GetPgClient(dbName string) *pgx.Conn {
	host := os.Getenv("PG_HOST")
	port := os.Getenv("PG_PORT")
	user := os.Getenv("PG_USER")
	password := os.Getenv("PG_PASSWORD")

	DATABASE_URL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, password, host, port, dbName)
	
	conn, err := pgx.Connect(context.Background(), DATABASE_URL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	return conn
}
