package main

import (
	"fmt"
	"time"

	"github.com/xtdlib/pgkv"
)

func main() {
	kv, err := pgkv.New[time.Time, int]("", "timetest")
	if err != nil {
		panic(err)
	}

	kv.Clear()

	kv.Set(time.Now(), 3)
	kv.Set(time.Now().Add(time.Hour), 4)

	for k, v := range kv.All {
		fmt.Println("k", k, "v", v)
	}
}
