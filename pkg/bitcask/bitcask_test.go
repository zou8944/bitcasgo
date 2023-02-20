package bitcask

import (
	"testing"
)

func TestBitCask_Put(t *testing.T) {
	basedir := "/Users/zouguodong/Code/Personal/bitcasgo"
	b, err := New(basedir, "bitcask")
	if err != nil {
		t.Fatal(err)
	}
	err = b.Put(1, 2)
	if err != nil {
		t.Fatal(err)
	}
}
