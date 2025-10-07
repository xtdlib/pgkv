package main

import (
	"fmt"
	"time"

	"github.com/xtdlib/pgkv"
)

func main() {
	kv := pgkv.New[time.Time, int]("", "timetest")

	kv.Clear()

	kv.Set(time.Now(), 3)
	kv.Set(time.Now().Add(time.Hour), 4)

	for k, v := range kv.All {
		fmt.Println("k", k, "v", v)
	}
}
