package bitfile

import (
	"fmt"
	"github.com/zou8944/bitcasgo/pkg/bitcask/serialization"
	"io/fs"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	Suffix     = ".bin"
	OldFileSep = "-"
)

type Manager struct {
	BaseDir   string
	FileName  string
	MaxNumber int32
	KeyDir    map[interface{}]ValueMeta
}

type ValueMeta struct {
	FileId      int32
	ValueType   serialization.VarType
	ValueSize   int32
	ValueOffset int64
	Timestamp   int32
}

func New(basedir, filename string) (*Manager, error) {
	fileinfos, err := ioutil.ReadDir(basedir)
	if err != nil {
		return nil, err
	}
	sort.Slice(fileinfos, func(i, j int) bool {
		return strings.Compare(fileinfos[i].Name(), fileinfos[j].Name()) > 0
	})
	latestOldFile := fileinfos[len(fileinfos)].Name()
	latestOldFileWithoutSuffix := strings.TrimSuffix(latestOldFile, Suffix)
	latestOldFileWithoutSuffixSegs := strings.Split(latestOldFileWithoutSuffix, OldFileSep)
	latestOldFileNumberStr := latestOldFileWithoutSuffixSegs[len(latestOldFileWithoutSuffixSegs)]
	latestOldFileNumber, err := strconv.Atoi(latestOldFileNumberStr)
	if err != nil {
		return nil, err
	}

	keyDir, err := scan(fileinfos)
	manager := &Manager{
		BaseDir:   basedir,
		FileName:  filename,
		MaxNumber: int32(latestOldFileNumber),
		KeyDir:    keyDir,
	}
	return manager, err
}

func scan(infos []fs.FileInfo) (map[interface{}]ValueMeta, error) {
	// 思路：没什么技巧，顺着往下读，塞入map，如果重复了就根据时间戳取舍，读完就OK
	keyDir := make(map[interface{}]ValueMeta)
	for _, info := range infos {
		file, err := os.Open(info.Name())
		if err != nil {
			return nil, err
		}

		offset := int64(0)
		stat, err := file.Stat()
		if err != nil {
			return nil, err
		}
		for {

			epochBytes, err := readBytes(file, 4, offset)
			keyTypeBytes, err := readBytes(file, 1, offset+4)
			keySizeBytes, err := readBytes(file, 4, offset+4+1)
			valueTypeBytes, err := readBytes(file, 1, offset+4+1+4)
			valueSizeBytes, err := readBytes(file, 4, offset+4+1+4+1)
			if err != nil {
				return nil, err
			}

			epochMillis, err := serialization.ParseInt32(epochBytes)
			keyType, err := serialization.ParseVarType(keyTypeBytes)
			keySize, err := serialization.ParseInt32(keySizeBytes)
			valueType, err := serialization.ParseVarType(valueTypeBytes)
			valueSize, err := serialization.ParseInt32(valueSizeBytes)
			if err != nil {
				return nil, err
			}

			keyBytes, err := readBytes(file, int(keySize), offset+4+1+4+1+4)
			if err != nil {
				return nil, err
			}

			key, err := serialization.DeserializeToken(keyType, keyBytes)
			if err != nil {
				return nil, err
			}

			newMeta := ValueMeta{
				FileId:      1,
				ValueType:   valueType,
				ValueSize:   valueSize,
				ValueOffset: offset + 4 + 1 + 4 + 1 + 4 + int64(keySize),
				Timestamp:   epochMillis,
			}

			if existMeta, ok := keyDir[key]; ok {
				if newMeta.Timestamp > existMeta.Timestamp {
					if newMeta.ValueSize == 0 {
						// delete
						delete(keyDir, key)
					} else {
						// update
						keyDir[key] = newMeta
					}
				}
			} else {
				// insert
				keyDir[key] = newMeta
			}

			offset += 4 + 1 + 4 + 1 + 4 + int64(keySize) + int64(valueSize)
			if offset >= stat.Size() {
				break
			}
		}
	}
	return keyDir, nil
}

func readBytes(file *os.File, n int, offset int64) ([]byte, error) {
	buf := make([]byte, n, n)
	_, err := file.ReadAt(buf, offset)
	return buf, err
}

func (m *Manager) ActiveFilePath() string {
	return fmt.Sprintf("%s/%s%s", m.BaseDir, m.FileName, Suffix)
}

func (m *Manager) GetValue(meta ValueMeta) ([]byte, error) {
	return nil, nil
}

func (m *Manager) PutValue(entryBytes []byte) (fileid int32, valueOffset int64, err error) {
	return 0, 0, err
}

func (m *Manager) TryMerge() error {
	return nil
}
