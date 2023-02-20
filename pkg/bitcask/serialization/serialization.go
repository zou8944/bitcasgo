package serialization

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type VarType byte

const (
	integer VarType = iota
	boolean
	floating
	strings
)

// Serialize key and value to binary byte slice entry, which will be store to file directly
func Serialize(key, value interface{}) (bytes []byte, valueType VarType, valueSize int32, valueOffset int64, err error) {
	keyType := getType(key)
	keyBytes, err := getBytes(keyType, key)
	if err != nil {
		return
	}
	keySize, keySizeBytes, err := getSize(keyBytes)
	if err != nil {
		return
	}

	valueType = getType(value)
	valueBytes, err := getBytes(valueType, value)
	if err != nil {
		return
	}
	valueSize, valueSizeBytes, err := getSize(valueBytes)
	if err != nil {
		return
	}

	valueOffset = int64(18 + keySize)

	epochBytes, err := toBytes(time.Now().UnixMilli())
	if err != nil {
		return
	}
	// TODO Add CRC

	bytes = stitching(epochBytes, keyType, valueType, keySizeBytes, valueSizeBytes, keyBytes, valueBytes)
	return
}

func SerializeTomb(key interface{}) ([]byte, error) {
	keyType := getType(key)
	keyBytes, err := getBytes(keyType, key)
	if err != nil {
		return nil, err
	}
	_, keySizeBytes, err := getSize(keyBytes)
	if err != nil {
		return nil, err
	}
	// tomb entry has no value info
	valueType := integer
	valueSizeBytes := []byte{}
	valueBytes := []byte{}
	epochBytes, err := toBytes(int32(time.Now().UnixMilli()))
	if err != nil {
		return nil, err
	}
	return stitching(epochBytes, keyType, valueType, keySizeBytes, valueSizeBytes, keyBytes, valueBytes), nil
}

// Deserialize a whole binary entry to key and value
func Deserialize(entry []byte) (key interface{}, value interface{}, err error) {
	keyType := VarType(entry[4])
	valueType := VarType(entry[5])
	keySizeBytes := entry[6:10]
	valueSizeBytes := entry[10:14]

	keySize, err := ParseInt32(keySizeBytes)
	if err != nil {
		return
	}
	valueSize, err := ParseInt32(valueSizeBytes)
	if err != nil {
		return
	}

	keyBytes := entry[14 : 14+keySize]
	valueBytes := entry[14+keySize : 14+keySize+valueSize]

	key, err = DeserializeToken(keyType, keyBytes)
	if err != nil {
		return
	}
	value, err = DeserializeToken(valueType, valueBytes)
	return
}

// TODO Support all type, including custom struct
func getType(token interface{}) VarType {
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

func getBytes(varType VarType, token interface{}) ([]byte, error) {
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

func getSize(valueBytes []byte) (int32, []byte, error) {
	length := int32(len(valueBytes))
	bytes, err := toBytes(length)
	return length, bytes, err
}

func DeserializeToken(tokenType VarType, tokenBytes []byte) (interface{}, error) {
	bf := bytes.NewBuffer(tokenBytes)
	switch tokenType {
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
		return string(tokenBytes), nil
	default:
		return nil, fmt.Errorf("unsupported var type. %d", tokenType)
	}
}

func stitching(epoch []byte, keyType, valueType VarType, keySize, valueSize, key, value []byte) []byte {
	var entryBytes []byte
	entryBytes = append(entryBytes, epoch...)
	entryBytes = append(entryBytes, byte(keyType))
	entryBytes = append(entryBytes, byte(keyType))
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

func ParseInt32(input []byte) (int32, error) {
	var r int32
	bf := bytes.NewBuffer(input)
	err := binary.Read(bf, binary.BigEndian, &r)
	return r, err
}

func ParseInt64(input []byte) (int64, error) {
	var r int64
	bf := bytes.NewBuffer(input)
	err := binary.Read(bf, binary.BigEndian, &r)
	return r, err
}

func ParseVarType(input []byte) (VarType, error) {
	switch input[0] {
	case byte(integer):
		return integer, nil
	case byte(boolean):
		return boolean, nil
	case byte(floating):
		return floating, nil
	case byte(strings):
		return strings, nil
	default:
		return 0, errors.New("invalid byte")
	}
}
