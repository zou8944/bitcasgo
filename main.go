package main

import "github.com/zou8944/bitcasgo/pkg/bitcask"

func main() {
	stub, err := bitcask.New("/Users/zouguodong/Code/Personal/bitcasgo", "bitcask")
	if err != nil {
		panic(err)
	}
	stub.Put("key", "value")
	stub.Get("key")
}
