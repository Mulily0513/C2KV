package directio

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ncw/directio"
)

const ioBlockSize = 4096

func writeDirectIO(path string, block []byte) error {
	out, err := directio.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = out.Write(block)
	return err
}

func readDirectIO(path string, block []byte) error {
	in, err := directio.OpenFile(path, os.O_RDONLY, 0o666)
	if err != nil {
		return err
	}
	defer in.Close()
	if _, err = in.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err = io.ReadFull(in, block)
	return err
}

func writeBufferIO(path string, block []byte) error {
	out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err = out.Write(block); err != nil {
		return err
	}
	return out.Sync()
}

func readBufferIO(path string, block []byte) error {
	in, err := os.OpenFile(path, os.O_RDONLY, 0o666)
	if err != nil {
		return err
	}
	defer in.Close()
	if _, err = in.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err = io.ReadFull(in, block)
	return err
}

func TestDirectIO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "direct_io_test")

	block1 := directio.AlignedBlock(ioBlockSize)
	for i := range block1 {
		block1[i] = 'A'
	}
	block2 := directio.AlignedBlock(ioBlockSize)

	if err := writeDirectIO(path, block1); err != nil {
		t.Fatalf("write direct io failed: %v", err)
	}
	if err := readDirectIO(path, block2); err != nil {
		t.Fatalf("read direct io failed: %v", err)
	}

	if !bytes.Equal(block1, block2) {
		t.Fatal("read data does not match written data for direct io")
	}
}

func TestBufferdIO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "buffer_io_test")

	block1 := make([]byte, ioBlockSize)
	for i := range block1 {
		block1[i] = 'A'
	}
	block2 := make([]byte, ioBlockSize)

	if err := writeBufferIO(path, block1); err != nil {
		t.Fatalf("write buffered io failed: %v", err)
	}
	if err := readBufferIO(path, block2); err != nil {
		t.Fatalf("read buffered io failed: %v", err)
	}

	if !bytes.Equal(block1, block2) {
		t.Fatal("read data does not match written data for buffered io")
	}
}

func TestDirectIOAndBufferIO(t *testing.T) {
	TestDirectIO(t)
	TestBufferdIO(t)
}
