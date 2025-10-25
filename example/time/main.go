package main

import (
	"log"
	"time"

	"github.com/xtdlib/pgkv"
)

func main() {
	kv := pgkv.New[time.Time, string]("testtime")
	_ = kv
	kv.Clear()

	now := time.Now()
	kv.Set(now, "hello")
	for k, v := range kv.All {
		_ = v
		if !k.Equal(now) {
			panic("key mismatch")
		}

		if v != "hello" {
			panic("value mismatch")
		}
		log.Println("key:", k, "value:", v)
	}
}
