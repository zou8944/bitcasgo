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

}

func TestManager_GetValue(t *testing.T) {

}

func TestManager_FilePath(t *testing.T) {

}
