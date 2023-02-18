package bitcask

import (
	"errors"
	"github.com/zou8944/bitcasgo/pkg/bitcask/bitfile"
	"github.com/zou8944/bitcasgo/pkg/bitcask/serialization"
)

type BitCask struct {
	index map[interface{}]bitfile.ValueMeta
}

func New(basedir, filename string) (*BitCask, error) {
	keyDir, err := bitfile.Scan(basedir, filename)
	if err != nil {
		return nil, err
	}
	return &BitCask{index: keyDir}, nil
}

func (b *BitCask) Get(key interface{}) (interface{}, error) {
	meta, ok := b.index[key]
	if !ok {
		return nil, errors.New("record not found")
	}
	valueBytes, err := bitfile.GetValue(meta)
	if err != nil {
		return nil, err
	}
	return serialization.DeserializeToken(meta.ValueType, valueBytes)
}

func (b *BitCask) Put(key, value interface{}) error {
	entryBytes, valueType, valueSize, err := serialization.Serialize(key, value)
	if err != nil {
		return err
	}
	fileid, offset, err := bitfile.PutValue(entryBytes)
	if err != nil {
		return err
	}
	b.index[key] = bitfile.ValueMeta{
		FileId:      fileid,
		ValueType:   valueType,
		ValueSize:   valueSize,
		ValueOffset: offset,
	}
	return nil
}

func (b *BitCask) Delete(key interface{}) error {
	entryBytes, err := serialization.SerializeTomb(key)
	if err != nil {
		return err
	}
	_, _, err = bitfile.PutValue(entryBytes)
	if err != nil {
		return err
	}
	if _, ok := b.index[key]; ok {
		delete(b.index, key)
	}
	return nil
}
