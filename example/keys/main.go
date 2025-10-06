package main

import (
	"log"
	"time"

	"github.com/xtdlib/pgkv"
	"github.com/xtdlib/rat"
)

func main() {
	kv, err := pgkv.New[string, *rat.Rational]("", "xxx")
	if err != nil {
		panic(err)
	}

	kv.Clear()

	log.Println(kv.Set("x", rat.Rat("0.1")))
	log.Println(kv.Set("y", rat.Rat("0.1")))
	log.Println(kv.Set("z", rat.Rat("0.1")))

	for k, v := range kv.All {
		time.Sleep(time.Second * 3)
		if k == "y" {
			kv.Set(k, rat.Rat("0.2"))
		}
		log.Println(k, v)
	}

	// for k, v := range kv.All {
	// 	log.Println(k, v)
	// }

}
