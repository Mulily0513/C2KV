package iobenchmark_test

import (
	"fmt"
	"github.com/ncw/directio"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

func BenchmarkDirectIO(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		DirectIO(GetTestData())
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N), "iterations")
}

func BenchmarkBufferdIO(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		BufferdIO(GetTestData())
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N), "iterations")
}

func DirectIO(testData []byte) {
	// Make a temporary file name
	fd, err := ioutil.TempFile("/Users/hlhf/GolandProjects/testdata", "direct_io_test")
	if err != nil {
		log.Fatal("Failed to make temp file", err)
	}
	path := fd.Name()
	fd.Close()

	fileDirectIO, err := directio.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal("Failed to open file for direct IO:", err)
	}
	defer fileDirectIO.Close()

	fileDirectIO.Write(testData)
}

func BufferdIO(testData []byte) {
	fd, err := ioutil.TempFile("/Users/hlhf/GolandProjects/testdata", "bufferd_io_test")
	if err != nil {
		log.Fatal("Failed to make temp file", err)
	}
	path := fd.Name()
	fd.Close()

	fileBufferedIO, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal("Failed to open file for buffered IO:", err)
	}
	defer fileBufferedIO.Close()

	fileBufferedIO.Write(testData)
	fileBufferedIO.Sync()
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
	testData := GetTestData()
	startTime := time.Now()
	directIOTime := time.Since(startTime)
	DirectIO(testData)
	fmt.Println("Direct IO time:", directIOTime)
}

func TestBufferdIO(t *testing.T) {
	testData := GetTestData()
	startTime := time.Now()
	bufferdIOTime := time.Since(startTime)
	BufferdIO(testData)
	fmt.Println("Buffered IO time:", bufferdIOTime)
}
