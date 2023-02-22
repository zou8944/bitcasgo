package bitcask

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func createTemp(t *testing.T) (string, string) {
	tempDir, err := os.MkdirTemp(os.TempDir(), "bitcask-*")
	if err != nil {
		t.Fatal(err)
	}
	return tempDir, "bitcask"
}

func TestBitCask_Put(t *testing.T) {

	t.Run("int to int", func(t *testing.T) {
		dir, filename := createTemp(t)
		b, err := New[int, int](dir, filename)
		if err != nil {
			t.Fatal(err)
		}
		k := 1
		v := 2
		if err := b.Put(k, v); err != nil {
			t.Fatal(err)
		}
		if av, err := b.Get(k); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, av, v)
		}
	})

	t.Run("string to string", func(t *testing.T) {
		dir, filename := createTemp(t)
		b, err := New[string, string](dir, filename)
		if err != nil {
			t.Fatal(err)
		}
		k := "key"
		v := "value"
		if err := b.Put(k, v); err != nil {
			t.Fatal(err)
		}
		if av, err := b.Get(k); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, av, v)
		}
	})

	t.Run("string to struct", func(t *testing.T) {
		type tstruct struct {
			Filed1 string
		}
		dir, filename := createTemp(t)
		b, err := New[string, tstruct](dir, filename)
		if err != nil {
			t.Fatal(err)
		}
		k := "key"
		v := tstruct{Filed1: "123"}
		if err := b.Put(k, v); err != nil {
			t.Fatal(err)
		}
		if av, err := b.Get(k); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, av, v)
		}
	})

}
