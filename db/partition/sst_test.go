package partition

import (
	"github.com/Mulily0513/C2KV/db/mocks"
	"os"
	"testing"
)

// todo more test unit
func TestSSTReadWrite(t *testing.T) {
	sst, err := OpenSST(createSSTFilePath(mocks.CurDirPath, NewSST, None))
	if err != nil {
		t.Errorf("Failed to open SST file: %v", err)
	}

	testData := []byte("test data")
	if err := sst.Write(testData); err != nil {
		t.Errorf("Failed to write to SST file: %v", err)
	}

	sst, err = OpenSST(sst.fp)
	readData, err := sst.Read(int64(len(testData)), 0)
	if err != nil {
		t.Errorf("Failed to read from SST file: %v", err)
	}

	if string(readData) != string(testData) {
		t.Errorf("Read data does not match written data")
	}
	if err = sst.Close(); err != nil {
		panic(err)
	}
	if err = sst.Remove(); err != nil {
		panic(err)
	}
}

func TestSSTRename(t *testing.T) {
	sst, err := OpenSST(createSSTFilePath(mocks.CurDirPath, TmpSST, None))
	if err != nil {
		t.Errorf("Failed to open SST file: %v", err)
	}

	if err = sst.Rename(createSSTFilePath(mocks.CurDirPath, NewSST, None)); err != nil {
		return
	}

	if _, err := os.Stat(sst.fp); os.IsNotExist(err) {
		t.Errorf("Renamed SST file does not exist")
	}
}
