package serialization

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"time"
)

// Serialize key and value to binary byte slice entry, which will be store to file directly
func Serialize(key, value interface{}) (bytes []byte, valueSize int32, valueOffset int64, err error) {
	// key
	keyBytes, err := GobBinaryMarshal(key)
	if err != nil {
		return
	}
	keySize := int32(len(keyBytes))
	keySizeBytes, err := SimpleBinaryMarshal(keySize)
	if err != nil {
		return
	}
	// value
	valueBytes, err := GobBinaryMarshal(value)
	if err != nil {
		return
	}
	valueSize = int32(len(valueBytes))
	valueSizeBytes, err := SimpleBinaryMarshal(valueSize)
	if err != nil {
		return
	}
	// value offset
	valueOffset = int64(16 + keySize)
	// epoch
	epochBytes, err := SimpleBinaryMarshal(time.Now().UnixMilli())
	if err != nil {
		return
	}
	// TODO Add CRC
	bytes = stitching(epochBytes, keySizeBytes, valueSizeBytes, keyBytes, valueBytes)
	return
}

func SerializeTomb(key interface{}) ([]byte, error) {
	// key
	keyBytes, err := GobBinaryMarshal(key)
	if err != nil {
		return nil, err
	}
	keySize := len(keyBytes)
	keySizeBytes, err := SimpleBinaryMarshal(int32(keySize))
	if err != nil {
		return nil, err
	}
	// tomb entry has no value info
	valueSizeBytes := []byte{}
	valueBytes := []byte{}
	// epoch
	epochBytes, err := SimpleBinaryMarshal(time.Now().UnixMilli())
	if err != nil {
		return nil, err
	}
	return stitching(epochBytes, keySizeBytes, valueSizeBytes, keyBytes, valueBytes), nil
}

func stitching(epoch []byte, keySize, valueSize, key, value []byte) []byte {
	var entryBytes []byte
	entryBytes = append(entryBytes, epoch...)
	entryBytes = append(entryBytes, keySize...)
	entryBytes = append(entryBytes, valueSize...)
	entryBytes = append(entryBytes, key...)
	entryBytes = append(entryBytes, value...)
	return entryBytes
}

// SimpleBinaryMarshal can only marshal numberic type and derived type with fixed length
func SimpleBinaryMarshal(token interface{}) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	err := binary.Write(buf, binary.BigEndian, token)
	return buf.Bytes(), err
}

// SimpleBinaryUnmarshal is the opposite of SimpleBinaryMarshal
func SimpleBinaryUnmarshal(bs []byte, v any) error {
	buf := bytes.NewBuffer(bs)
	return binary.Read(buf, binary.BigEndian, v)
}

// GobBinaryMarshal can marshal any type
func GobBinaryMarshal(token interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(token)
	return buf.Bytes(), err
}

// GobBinaryUnmarshal is the opposite of GobBinaryMarshal
func GobBinaryUnmarshal(bs []byte, v any) error {
	buf := bytes.NewBuffer(bs)
	dec := gob.NewDecoder(buf)
	return dec.Decode(v)
}
