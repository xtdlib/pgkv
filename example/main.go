package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xtdlib/pgkv"
)

func main() {
	urlExample := "postgres://postgres:postgres@localhost:5432/postgres"
	conn, err := pgx.Connect(context.Background(), urlExample)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	kv, err := pgkv.New[string, int](conn, "sample")
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	kv.Set(ctx, "wer", 0)
	log.Println(kv.Get(ctx, "wer"))
	for i := 0; i < 1000; i++ {
		go func() {
			_, err := kv.Add(ctx, "wer", int64(i))
			if err != nil {
				panic(err)
			}
		}()
	}
	time.Sleep(time.Second * 5)
	// log.Println(kv.Add(ctx, "wer", 1))

	log.Println(kv.Get(ctx, "wer"))
}
