package bitfile

import (
	"fmt"
	"github.com/zou8944/bitcasgo/pkg/bitcask/serialization"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	Suffix      = ".bin"
	IDSep       = "-"
	B           = 1
	KB          = 1024 * B
	MB          = 1024 * KB
	MaxFileSize = 100 * MB
)

type Manager struct {
	BaseDir      string
	FileName     string
	ActiveFileId int32
	KeyDir       map[interface{}]ValueMeta
}

type ValueMeta struct {
	FileId      int32
	ValueType   serialization.VarType
	ValueSize   int32
	ValueOffset int64
	Timestamp   int32
}

func New(basedir, filename string) (*Manager, error) {
	activeId, err := GetActiveFileId(basedir, filename)
	if err != nil {
		return nil, err
	}
	keyDir, err := BuildIndexFromFile(basedir, filename)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		BaseDir:      basedir,
		FileName:     filename,
		ActiveFileId: int32(activeId),
		KeyDir:       keyDir,
	}
	return m, nil
}

// GetActiveFileId get the number of current active WAL file, start from 1
func GetActiveFileId(basedir, filename string) (int, error) {
	fis, err := ioutil.ReadDir(basedir)
	if err != nil {
		return 0, err
	}
	var activeId int
	for _, info := range fis {
		baseName := filepath.Base(info.Name())
		if strings.HasPrefix(baseName, filename+IDSep) && strings.HasSuffix(baseName, Suffix) {
			idStr := strings.TrimSuffix(strings.TrimPrefix(baseName, filename+IDSep), Suffix)
			id, err := strconv.Atoi(idStr)
			if err != nil {
				return 0, err
			}
			if id > activeId {
				activeId = id
			}
		}
	}
	if activeId == 0 {
		activeId = 1
	}
	return activeId, nil
}

func BuildIndexFromFile(basedir, filename string) (map[interface{}]ValueMeta, error) {
	fis, err := ioutil.ReadDir(basedir)
	if err != nil {
		return nil, err
	}
	keyDir := make(map[interface{}]ValueMeta)
	for _, info := range fis {
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
			valueTypeBytes, err := readBytes(file, 1, offset+4+1)
			keySizeBytes, err := readBytes(file, 4, offset+4+1+1)
			valueSizeBytes, err := readBytes(file, 4, offset+4+1+1+4)
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

			keyBytes, err := readBytes(file, int(keySize), offset+4+1+1+4+4)
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
				ValueOffset: offset + 4 + 1 + 1 + 4 + 4 + int64(keySize),
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

func (m *Manager) FilePath(fileid int32) string {
	return fmt.Sprintf("%s/%s-%d%s", m.BaseDir, m.FileName, fileid, Suffix)
}

func (m *Manager) GetValue(meta ValueMeta) ([]byte, error) {
	file, err := os.Open(m.FilePath(meta.FileId))
	defer func() {
		_ = file.Close()
	}()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, meta.ValueSize, meta.ValueSize)
	_, err = file.ReadAt(buf, meta.ValueOffset)
	return buf, err
}

func (m *Manager) PutValue(entryBytes []byte) (fileid int32, entryOffset int64, err error) {
	activeFileStat, err := os.Stat(m.FilePath(m.ActiveFileId))
	if err != nil {
		return
	}
	if activeFileStat.Size() > MaxFileSize {
		m.ActiveFileId++
	}
	activeFile, err := os.OpenFile(m.FilePath(m.ActiveFileId), os.O_RDWR, os.ModeAppend)
	if err != nil {
		return
	}
	activeFileStat, err = activeFile.Stat()
	if err != nil {
		return
	}
	offset := activeFileStat.Size()
	_, err = activeFile.Write(entryBytes)
	return m.ActiveFileId, offset, err
}

func (m *Manager) TryMerge() error {
	return nil
}
