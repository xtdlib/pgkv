package main

import (
	"fmt"
	"time"

	"github.com/xtdlib/pgkv"
)

func main() {
	kv := pgkv.New[string, time.Time]("", "tim2etest2")
	kv2 := pgkv.New[time.Time, string]("", "tim2etest23")
	kv.Clear()
	kv2.Clear()

	kv.Set("1", time.Now())
	kv2.Set(time.Now(), "1")

	for _, v := range kv.All {
		fmt.Println(v.Local())
	}

	for k, _ := range kv2.All {
		fmt.Println(k.Local())
	}
}
