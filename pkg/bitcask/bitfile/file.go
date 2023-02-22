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

	EpochBytes      = 8
	SizeBytes       = 4
	EpochOffset     = 0
	KeySizeOffset   = EpochOffset + EpochBytes
	ValueSizeOffset = EpochOffset + EpochBytes + SizeBytes
	KeyOffset       = EpochOffset + EpochBytes + SizeBytes + SizeBytes
)

type Manager struct {
	BaseDir      string
	FileName     string
	ActiveFileId int
	KeyDir       map[interface{}]ValueMeta
}

type ValueMeta struct {
	FileId      int
	ValueSize   int32
	ValueOffset int64
	Timestamp   int64
}

func New[K any](basedir, filename string) (*Manager, error) {
	activeId, err := GetActiveFileId(basedir, filename)
	if err != nil {
		return nil, err
	}
	keyDir, err := BuildIndexFromFile[K](basedir, filename)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		BaseDir:      basedir,
		FileName:     filename,
		ActiveFileId: activeId,
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

func BuildIndexFromFile[K any](basedir, filename string) (map[interface{}]ValueMeta, error) {
	fis, err := ioutil.ReadDir(basedir)
	if err != nil {
		return nil, err
	}
	keyDir := make(map[interface{}]ValueMeta)
	for _, info := range fis {
		absolutePath := fmt.Sprintf("%s/%s", basedir, info.Name())
		baseName := info.Name()
		// only data file need to handle
		if !strings.HasPrefix(baseName, filename+IDSep) || !strings.HasSuffix(baseName, Suffix) {
			continue
		}
		// retrieve file id
		fileidStr := strings.TrimSuffix(strings.TrimPrefix(baseName, filename+IDSep), Suffix)
		fileid, err := strconv.Atoi(fileidStr)
		if err != nil {
			return nil, err
		}
		// open and read
		file, err := os.Open(absolutePath)
		if err != nil {
			return nil, err
		}

		offsetBase := int64(0)
		stat, err := file.Stat()
		if err != nil {
			return nil, err
		}
		for {

			epochBytes, err := readBytes(file, EpochBytes, offsetBase+EpochOffset)
			keySizeBytes, err := readBytes(file, SizeBytes, offsetBase+KeySizeOffset)
			valueSizeBytes, err := readBytes(file, SizeBytes, offsetBase+ValueSizeOffset)
			if err != nil {
				return nil, err
			}

			var epochMillis int64
			var keySize int32
			var valueSize int32
			var key K
			err = serialization.SimpleBinaryUnmarshal(epochBytes, &epochMillis)
			err = serialization.SimpleBinaryUnmarshal(keySizeBytes, &keySize)
			err = serialization.SimpleBinaryUnmarshal(valueSizeBytes, &valueSize)
			if err != nil {
				return nil, err
			}
			keyBytes, err := readBytes(file, int(keySize), offsetBase+KeyOffset)
			if err != nil {
				return nil, err
			}
			err = serialization.GobBinaryUnmarshal(keyBytes, &key)
			if err != nil {
				return nil, err
			}

			newMeta := ValueMeta{
				FileId:      fileid,
				ValueSize:   valueSize,
				ValueOffset: offsetBase + KeyOffset + int64(keySize),
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

			offsetBase += KeyOffset + int64(keySize) + int64(valueSize)
			if offsetBase >= stat.Size() {
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

func (m *Manager) FilePath(fileid int) string {
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

func (m *Manager) PutValue(entryBytes []byte) (fileid int, entryOffset int64, err error) {
	activefile, err := m.activeFile()
	if err != nil {
		return
	}
	defer func() { _ = activefile.Close() }()
	activefilestat, err := activefile.Stat()
	if err != nil {
		return
	}
	offset := activefilestat.Size()
	_, err = activefile.Write(entryBytes)
	return m.ActiveFileId, offset, err
}

func (m *Manager) activeFile() (*os.File, error) {
	activefilepath := m.FilePath(m.ActiveFileId)
	activefilestat, err := os.Stat(activefilepath)
	if err != nil {
		if os.IsNotExist(err) {
			return os.Create(activefilepath)
		} else {
			return nil, err
		}
	}
	if activefilestat.Size() > MaxFileSize {
		m.ActiveFileId++
		activefilepath = m.FilePath(m.ActiveFileId)
		return os.Create(activefilepath)
	}
	return os.OpenFile(activefilepath, os.O_WRONLY|os.O_APPEND, os.ModePerm)
}

func (m *Manager) TryMerge() error {
	return nil
}
