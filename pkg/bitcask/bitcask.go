package bitcask

import (
	"errors"
	"fmt"
	"github.com/robfig/cron"
	"github.com/zou8944/bitcasgo/pkg/bitcask/bitfile"
	"github.com/zou8944/bitcasgo/pkg/bitcask/serialization"
)

type BitCask struct {
	index       map[interface{}]bitfile.ValueMeta
	fileManager *bitfile.Manager
}

func New(basedir, filename string) (*BitCask, error) {
	manager, err := bitfile.New(basedir, filename)
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

	cask := &BitCask{
		index:       manager.KeyDir,
		fileManager: manager,
	}
	return cask, nil
}

func (b *BitCask) Get(key interface{}) (interface{}, error) {
	meta, ok := b.index[key]
	if !ok {
		return nil, errors.New("record not found")
	}
	valueBytes, err := b.fileManager.GetValue(meta)
	if err != nil {
		return nil, err
	}
	return serialization.DeserializeToken(meta.ValueType, valueBytes)
}

func (b *BitCask) Put(key, value interface{}) error {
	entryBytes, valueType, valueOffsetInEntry, valueSize, err := serialization.Serialize(key, value)
	if err != nil {
		return err
	}
	fileid, entryOffset, err := b.fileManager.PutValue(entryBytes)
	if err != nil {
		return err
	}
	b.index[key] = bitfile.ValueMeta{
		FileId:      fileid,
		ValueType:   valueType,
		ValueSize:   valueSize,
		ValueOffset: entryOffset + int64(valueOffsetInEntry),
	}
	return nil
}

func (b *BitCask) Delete(key interface{}) error {
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
