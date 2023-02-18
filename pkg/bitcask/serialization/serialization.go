package serialization

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"
)

const (
	integer byte = iota
	boolean
	floating
	strings
)

// Serialize key and value to binary byte slice entry, which will be store to file directly
func Serialize(key, value interface{}) ([]byte, error) {
	keyType := getType(key)
	keyBytes, err := getBytes(keyType, key)
	if err != nil {
		return nil, err
	}
	keySize, err := getSize(keyBytes)
	if err != nil {
		return nil, err
	}

	valueType := getType(value)
	valueBytes, err := getBytes(valueType, value)
	if err != nil {
		return nil, err
	}
	valueSize, err := getSize(valueBytes)
	if err != nil {
		return nil, err
	}

	epochBytes, err := toBytes(int32(time.Now().UnixMilli()))
	if err != nil {
		return nil, err
	}
	// TODO Add CRC

	return stitching(epochBytes, keyType, valueType, keySize, valueSize, keyBytes, valueBytes), nil
}

// Deserialize a whole binary entry to key and value
func Deserialize(entry []byte) (key interface{}, value interface{}, err error) {
	keyType := entry[4]
	valueType := entry[5]
	keySizeBytes := entry[6:10]
	valueSizeBytes := entry[10:14]

	keySize, err := parseInt32(keySizeBytes)
	if err != nil {
		return
	}
	valueSize, err := parseInt32(valueSizeBytes)
	if err != nil {
		return
	}

	keyBytes := entry[14 : 14+keySize]
	valueBytes := entry[14+keySize : 14+keySize+valueSize]

	key, err = parseValueFromBytes(keyType, keyBytes)
	if err != nil {
		return
	}
	value, err = parseValueFromBytes(valueType, valueBytes)
	return
}

// TODO Support all type, including custom struct
func getType(token interface{}) byte {
	switch token.(type) {
	case int8, int, int16, int32, int64, uint8, uint, uint16, uint32, uint64:
		return integer
	case float32, float64:
		return floating
	case bool:
		return boolean
	default:
		return strings
	}
}

func getBytes(varType byte, token interface{}) ([]byte, error) {
	input := token
	if varType == strings {
		if jsons, err := json.Marshal(token); err != nil {
			return nil, err
		} else {
			input = jsons
		}
	}
	return toBytes(input)
}

func getSize(valueBytes []byte) ([]byte, error) {
	length := int32(len(valueBytes))
	return toBytes(length)
}

func parseValueFromBytes(varType byte, binValue []byte) (interface{}, error) {
	bf := bytes.NewBuffer(binValue)
	switch varType {
	case boolean:
		var r bool
		err := binary.Read(bf, binary.BigEndian, &r)
		return r, err
	case integer:
		var r int64
		err := binary.Read(bf, binary.BigEndian, &r)
		return r, err
	case floating:
		var r float64
		err := binary.Read(bf, binary.BigEndian, &r)
		return r, err
	case strings:
		return string(binValue), nil
	default:
		return nil, fmt.Errorf("unsupported var type. %d", varType)
	}
}

func stitching(epoch []byte, keyType, valueType byte, keySize, valueSize, key, value []byte) []byte {
	var entryBytes []byte
	entryBytes = append(entryBytes, epoch...)
	entryBytes = append(entryBytes, keyType)
	entryBytes = append(entryBytes, valueType)
	entryBytes = append(entryBytes, keySize...)
	entryBytes = append(entryBytes, valueSize...)
	entryBytes = append(entryBytes, key...)
	entryBytes = append(entryBytes, value...)
	return entryBytes
}

func toBytes(input interface{}) ([]byte, error) {
	if in, ok := input.(int); ok {
		input = int64(in)
	}
	if in, ok := input.(uint); ok {
		input = uint64(in)
	}
	bf := bytes.NewBuffer([]byte{})
	err := binary.Write(bf, binary.BigEndian, input)
	return bf.Bytes(), err
}

func parseInt32(input []byte) (int32, error) {
	var r int32
	bf := bytes.NewBuffer(input)
	err := binary.Read(bf, binary.BigEndian, &r)
	return r, err
}
