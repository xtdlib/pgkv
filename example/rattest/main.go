package main

import (
	"log"

	"github.com/xtdlib/pgkv"
	"github.com/xtdlib/rat"
)

func main() {
	kv := pgkv.New[*rat.Rational, *rat.Rational]("", "xxx")

	key := rat.Rat(0)

	kv.Clear()

	kv.AddRat(key, "1")
	kv.AddRat(key, "2")
	kv.AddRat(key, "3")
	kv.AddRat(key, rat.Rat("4").Mul(-1))
	log.Println(kv.Get(key))

}
