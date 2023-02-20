package serialization

import (
	"bytes"
	"encoding/binary"
	"time"
)

// Serialize key and value to binary byte slice entry, which will be store to file directly
func Serialize(key, value interface{}) (bytes []byte, valueSize int32, valueOffset int64, err error) {
	// key
	keyBytes, err := BinaryMarshal(key)
	if err != nil {
		return
	}
	keySize := len(keyBytes)
	keySizeBytes, err := BinaryMarshal(int32(keySize))
	if err != nil {
		return
	}
	// value
	valueBytes, err := BinaryMarshal(value)
	if err != nil {
		return
	}
	valueSize = int32(len(valueBytes))
	valueSizeBytes, err := BinaryMarshal(int32(valueSize))
	if err != nil {
		return
	}
	// value offset
	valueOffset = int64(16 + keySize)
	// epoch
	epochBytes, err := BinaryMarshal(time.Now().UnixMilli())
	if err != nil {
		return
	}
	// TODO Add CRC
	bytes = stitching(epochBytes, keySizeBytes, valueSizeBytes, keyBytes, valueBytes)
	return
}

func SerializeTomb(key interface{}) ([]byte, error) {
	// key
	keyBytes, err := BinaryMarshal(key)
	if err != nil {
		return nil, err
	}
	keySize := len(keyBytes)
	keySizeBytes, err := BinaryMarshal(int32(keySize))
	if err != nil {
		return nil, err
	}
	// tomb entry has no value info
	valueSizeBytes := []byte{}
	valueBytes := []byte{}
	// epoch
	epochBytes, err := BinaryMarshal(time.Now().UnixMilli())
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

func BinaryMarshal(token interface{}) ([]byte, error) {
	if _token, ok := token.(int); ok {
		token = int64(_token)
	}
	if _token, ok := token.(uint); ok {
		token = uint64(_token)
	}
	buf := bytes.NewBuffer([]byte{})
	err := binary.Write(buf, binary.BigEndian, token)
	return buf.Bytes(), err
}

func BinaryUnmarshal(bs []byte, v any) error {
	buf := bytes.NewBuffer(bs)
	if _v, ok := v.(*int); ok {
		var vv int64
		err := binary.Read(buf, binary.BigEndian, &vv)
		*_v = int(vv)
		return err
	}
	if _v, ok := v.(*uint); ok {
		var vv uint64
		err := binary.Read(buf, binary.BigEndian, &vv)
		*_v = uint(vv)
		return err
	}
	return binary.Read(buf, binary.BigEndian, v)
}
