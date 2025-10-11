package main

import (
	"log/slog"

	"github.com/xtdlib/pgkv"
	"github.com/xtdlib/rat"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	kv := pgkv.New[*rat.Rational, *rat.Rational]("", "same")

	key := rat.Rat(0)

	kv.Clear()
	kv.Set(key, rat.Rat(3))
	kv.Set(key, rat.Rat(4))

	kv.AddRat(key, "1/3")
	kv.AddRat(key, "1/3")
	kv.AddRat(key, "1/3")
	// if !kv.Get(key).Equal("2/3") {
	// 	log.Fatal("error")
	// }
	//
	// kv.AddRat(rat.Rat(1), "1")
	// kv.AddRat(rat.Rat(2), "2")
	//
	// if !kv.Get(rat.Rat(1)).Equal("1") {
	// 	log.Fatal("error")
	// }
	// if !kv.Get(rat.Rat(2)).Equal("2") {
	// 	log.Fatal("error")
	// }
	//
	// for k, v := range kv.All {
	// 	log.Println(k, v)
	// }
}
