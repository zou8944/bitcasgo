package bitfile

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestGetActiveFileId(t *testing.T) {
	t.Run("Empty dir", func(t *testing.T) {
		dir := os.TempDir() + "bitcask"
		_ = os.Mkdir(dir, os.ModePerm)
		defer func() { _ = os.Remove(dir) }()
		name := "bitcask"

		activeId, err := GetActiveFileId(dir, name)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, 1, activeId)
	})
	t.Run("Dir with non related files", func(t *testing.T) {
		dir := os.TempDir() + "bitcask"
		nonRelatedFile1 := dir + "/bitcask0.bin"
		nonRelatedFile2 := dir + "/bitcask1"
		name := "bitcask"
		_ = os.Mkdir(dir, os.ModePerm)
		_, _ = os.Create(nonRelatedFile1)
		_, _ = os.Create(nonRelatedFile2)
		defer func() {
			_ = os.RemoveAll(dir)
		}()

		activeId, err := GetActiveFileId(dir, name)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, 1, activeId)
	})
	t.Run("Dir with related files", func(t *testing.T) {
		dir := os.TempDir() + "bitcask"
		nonRelatedFile1 := dir + "/bitcask-1.bin"
		nonRelatedFile2 := dir + "/bitcask-2.bin"
		name := "bitcask"
		_ = os.Mkdir(dir, os.ModePerm)
		_, _ = os.Create(nonRelatedFile1)
		_, _ = os.Create(nonRelatedFile2)
		defer func() {
			_ = os.RemoveAll(dir)
		}()

		activeId, err := GetActiveFileId(dir, name)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, 2, activeId)
	})
}

func TestManager_PutValue(t *testing.T) {
	t.Run("Active file not exist", func(t *testing.T) {
		// prepare files and write test byte data
		baseDir := os.TempDir() + "bitcask"
		filename := "bitcask"
		_ = os.Mkdir(baseDir, os.ModePerm)
		defer func() {
			_ = os.RemoveAll(baseDir)
		}()

		// mock object
		mockManager := Manager{
			BaseDir:      baseDir,
			FileName:     filename,
			ActiveFileId: 1,
		}

		// put and assert
		bytes := []byte{1, 2, 3, 4, 5, 6, 7}
		fileid, offset, err := mockManager.PutValue(bytes)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, 1, int(fileid))
		assert.Equal(t, 0, int(offset))
	})
	t.Run("Active file full", func(t *testing.T) {
		// prepare files and write test byte data
		baseDir := os.TempDir() + "bitcask"
		filename := "bitcask"
		activefilepath := baseDir + "/bitcask-1.bin"
		_ = os.Mkdir(baseDir, os.ModePerm)
		activefile, _ := os.Create(activefilepath)
		buf := make([]byte, MaxFileSize+1*KB)
		_, _ = activefile.Write(buf)
		defer func() {
			_ = os.RemoveAll(baseDir)
		}()

		// mock object
		mockManager := Manager{
			BaseDir:      baseDir,
			FileName:     filename,
			ActiveFileId: 1,
		}

		fileid, offset, err := mockManager.PutValue([]byte{111, 1, 1})
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, 2, int(fileid))
		assert.Equal(t, 0, int(offset))
	})
	t.Run("Active file exist and not full", func(t *testing.T) {
		// prepare files and write test byte data
		baseDir := os.TempDir() + "bitcask"
		filename := "bitcask"
		activefilepath := baseDir + "/bitcask-1.bin"
		_ = os.Mkdir(baseDir, os.ModePerm)
		activefile, _ := os.Create(activefilepath)
		buf := make([]byte, 3000)
		_, _ = activefile.Write(buf)
		defer func() {
			_ = os.RemoveAll(baseDir)
		}()

		// mock object
		mockManager := Manager{
			BaseDir:      baseDir,
			FileName:     filename,
			ActiveFileId: 1,
		}

		fileid, offset, err := mockManager.PutValue([]byte{111, 1, 1})
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, 1, int(fileid))
		assert.Equal(t, 3000, int(offset))
	})
}

func TestManager_GetValue(t *testing.T) {
	prefixBytes := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	targetBytes := []byte{1, 0, 1, 11, 21}

	// prepare files and write test byte data
	baseDir := os.TempDir() + "bitcask"
	filename := "bitcask"
	_ = os.Mkdir(baseDir, os.ModePerm)
	activeFile, _ := os.Create(baseDir + "/bitcask-1.bin")
	_, _ = activeFile.Write(append(prefixBytes, targetBytes...))
	defer func() {
		_ = os.RemoveAll(baseDir)
	}()

	// mock object
	mockManager := Manager{
		BaseDir:      baseDir,
		FileName:     filename,
		ActiveFileId: 1,
	}
	mockMeta := ValueMeta{
		FileId:      1,
		ValueSize:   int32(len(targetBytes)),
		ValueOffset: int64(len(prefixBytes)),
	}

	// try and assert
	actualBytes, err := mockManager.GetValue(mockMeta)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, targetBytes, actualBytes)
}

func TestManager_FilePath(t *testing.T) {
	m := Manager{
		BaseDir:      "/users/demo/temp/bitcask",
		FileName:     "bitcask",
		ActiveFileId: 1,
	}
	path := m.FilePath(2)

	assert.Equal(t, "/users/demo/temp/bitcask/bitcask-2.bin", path)
}
