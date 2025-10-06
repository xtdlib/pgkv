package main

import (
	"context"
	"log"

	"github.com/xtdlib/pgkv"
	"github.com/xtdlib/rat"
)

func main() {
	kv, err := pgkv.New[string, *rat.Rational]("", "sample")
	if err != nil {
		panic(err)
	}

	key := "wrwee1wer"

	ctx := context.Background()
	{
		out, err := kv.AddRat(ctx, key, "1/3")
		_ = out
		if err != nil {
			panic(err)
		}
		log.Println("Added value:", out)
	}

	{
		out, err := kv.AddRat(ctx, key, "1/3")
		_ = out
		if err != nil {
			panic(err)
		}
		log.Println("Added value:", out)
	}
}
