package bitcask

import (
	"errors"
	"fmt"
	"github.com/robfig/cron"
	"github.com/zou8944/bitcasgo/pkg/bitcask/bitfile"
	"github.com/zou8944/bitcasgo/pkg/bitcask/serialization"
)

type BitCask[K any, V any] struct {
	index       map[interface{}]bitfile.ValueMeta
	fileManager *bitfile.Manager
}

func New[K any, V any](basedir, filename string) (*BitCask[K, V], error) {
	manager, err := bitfile.New[K](basedir, filename)
	if err != nil {
		return nil, err
	}
	c := cron.New()
	err = c.AddFunc("0 0 15 * * *", func() {
		err := manager.TryMerge()
		if err != nil {
			fmt.Printf("try merge fail. %v", err)
		}
	})
	if err != nil {
		return nil, err
	}
	c.Start()

	cask := &BitCask[K, V]{
		index:       manager.KeyDir,
		fileManager: manager,
	}
	return cask, nil
}

func (b *BitCask[K, V]) Get(key interface{}) (interface{}, error) {
	meta, ok := b.index[key]
	if !ok {
		return nil, errors.New("record not found")
	}
	valueBytes, err := b.fileManager.GetValue(meta)
	if err != nil {
		return nil, err
	}
	var v V
	err = serialization.BinaryUnmarshal(valueBytes, &v)
	return v, err
}

func (b *BitCask[K, V]) Put(key K, value V) error {
	entryBytes, valueSize, valueOffsetInEntry, err := serialization.Serialize(key, value)
	if err != nil {
		return err
	}
	fileid, entryOffset, err := b.fileManager.PutValue(entryBytes)
	if err != nil {
		return err
	}
	b.index[key] = bitfile.ValueMeta{
		FileId:      fileid,
		ValueSize:   int32(valueSize),
		ValueOffset: entryOffset + int64(valueOffsetInEntry),
	}
	return nil
}

func (b *BitCask[K, V]) Delete(key interface{}) error {
	entryBytes, err := serialization.SerializeTomb(key)
	if err != nil {
		return err
	}
	_, _, err = b.fileManager.PutValue(entryBytes)
	if err != nil {
		return err
	}
	if _, ok := b.index[key]; ok {
		delete(b.index, key)
	}
	return nil
}
