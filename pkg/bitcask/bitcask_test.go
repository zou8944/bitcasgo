package bitcask

import (
	"fmt"
	"os"
	"testing"
)

func TestBitCask_Put(t *testing.T) {
	basedir := "/Users/zouguodong/Code/Personal/bitcasgo"
	b, err := New[int, float32](basedir, "bitcask")
	defer func() {
		_ = os.RemoveAll(basedir + "/bitcask-1.bin")
	}()
	if err != nil {
		t.Fatal(err)
	}
	err = b.Put(1, 1.1)
	if err != nil {
		t.Fatal(err)
	}
	value, err := b.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%f\n", value)
}
