package bitfile

import "github.com/zou8944/bitcasgo/pkg/bitcask/serialization"

const (
	Suffix = ".bin"
)

type ValueMeta struct {
	FileId      int32
	ValueType   serialization.VarType
	ValueSize   int32
	ValueOffset int64
	Timestamp   int64
}

func Scan(basedir, filename string) (map[interface{}]ValueMeta, error) {
	return nil, nil
}

func GetValue(meta ValueMeta) ([]byte, error) {
	return nil, nil
}

func PutValue(entryBytes []byte) (fileid int32, valueOffset int64, err error) {
	return 0, 0, err
}

func TryMerge() error {
	return nil
}
