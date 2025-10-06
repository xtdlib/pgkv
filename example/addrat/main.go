package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xtdlib/pgkv"
	"github.com/xtdlib/rat"
)

func main() {
	urlExample := "postgres://postgres:postgres@localhost:5432/postgres"
	pool, err := pgxpool.New(context.Background(), urlExample)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	kv, err := pgkv.New[string, *rat.Rational](pool, "sample")
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	kv.Set(ctx, "wer", rat.Rat(0))
	log.Println(kv.Get(ctx, "wer"))

	var wg sync.WaitGroup
	for i := 0; i < 1001; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// _, err := kv.Add(ctx, "wer", 1)
			// if err != nil {
			// 	panic(err)
			// }
			// kv.Update(ctx, "wer", func(old *rat.Rational) *rat.Rational {
			// 	return old.AddRat("0.3")
			// })

			_, err := kv.AddRat(ctx, "wer", "0.1")
			if err != nil {
				panic(err)
			}

		}()
	}
	wg.Wait()

	// it should be 1000
	log.Println(kv.Get(ctx, "wer"))
}
