package main

import (
	"github.com/xtdlib/pgkv"
)

type Human struct {
	Name string
	Age  int
}

func main() {
	kv := pgkv.New[string, Human]("", "structtest")

	kv.Clear()

	kv.Set("a", Human{Name: "Alice", Age: 30})
	kv.Set("b", Human{Name: "Brian", Age: 30})

	h := kv.Get("a")
	if h.Name != "Alice" || h.Age != 30 {
		panic("Get failed")
	}

}
