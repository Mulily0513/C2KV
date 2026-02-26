package iobenchmark_test

import (
	"fmt"
	"github.com/ncw/directio"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkDirectIO(b *testing.B) {
	dir := b.TempDir()
	testData := GetTestData()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := DirectIO(dir, testData); err != nil {
			b.Fatalf("direct io benchmark failed: %v", err)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N), "iterations")
}

func BenchmarkBufferdIO(b *testing.B) {
	dir := b.TempDir()
	testData := GetTestData()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := BufferdIO(dir, testData); err != nil {
			b.Fatalf("buffered io benchmark failed: %v", err)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N), "iterations")
}

func DirectIO(dir string, testData []byte) error {
	path := filepath.Join(dir, fmt.Sprintf("direct_io_test_%d", time.Now().UnixNano()))
	fileDirectIO, err := directio.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer fileDirectIO.Close()

	if _, err = fileDirectIO.Write(testData); err != nil {
		return err
	}
	return nil
}

func BufferdIO(dir string, testData []byte) error {
	path := filepath.Join(dir, fmt.Sprintf("buffered_io_test_%d", time.Now().UnixNano()))
	fileBufferedIO, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer fileBufferedIO.Close()

	if _, err = fileBufferedIO.Write(testData); err != nil {
		return err
	}
	return fileBufferedIO.Sync()
}

func GetTestData() []byte {
	blockSize := 4096
	blockCount := 1
	testData := make([]byte, blockSize*blockCount)
	for i := 0; i < len(testData); i++ {
		testData[i] = 1
	}
	return testData
}

func TestDirectIO(t *testing.T) {
	dir := t.TempDir()
	testData := GetTestData()
	startTime := time.Now()
	if err := DirectIO(dir, testData); err != nil {
		t.Fatalf("Direct IO failed: %v", err)
	}
	directIOTime := time.Since(startTime)
	fmt.Println("Direct IO time:", directIOTime)
}

func TestBufferdIO(t *testing.T) {
	dir := t.TempDir()
	testData := GetTestData()
	startTime := time.Now()
	if err := BufferdIO(dir, testData); err != nil {
		t.Fatalf("Buffered IO failed: %v", err)
	}
	bufferdIOTime := time.Since(startTime)
	fmt.Println("Buffered IO time:", bufferdIOTime)
}

func TestDirectIORoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "direct-roundtrip")
	testData := directio.AlignedBlock(4096)
	for i := range testData {
		testData[i] = 1
	}

	file, err := directio.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		t.Fatalf("open direct file for write failed: %v", err)
	}
	if _, err = file.Write(testData); err != nil {
		_ = file.Close()
		t.Fatalf("write direct file failed: %v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("close direct file failed: %v", err)
	}

	in, err := directio.OpenFile(path, os.O_RDONLY, 0o666)
	if err != nil {
		t.Fatalf("open direct file for read failed: %v", err)
	}
	defer in.Close()
	got := directio.AlignedBlock(4096)
	if _, err = io.ReadFull(in, got); err != nil {
		t.Fatalf("read direct file failed: %v", err)
	}
	for i := range testData {
		if testData[i] != got[i] {
			t.Fatalf("direct io roundtrip mismatch at %d", i)
		}
	}
}
