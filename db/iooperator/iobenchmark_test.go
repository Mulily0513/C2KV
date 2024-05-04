package iooperator

import (
	"fmt"
	"github.com/Mulily0513/C2KV/db/iooperator/directio"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

func GetTestData() []byte {
	// 创建测试数据
	blockSize := 4098
	blockCount := 1
	testData := make([]byte, blockSize*blockCount)
	for i := 0; i < len(testData); i++ {
		testData[i] = 1
	}
	return testData
}

func BenchmarkDirectIO(b *testing.B) {
	b.ResetTimer() // 重置计时器，排除初始化代码的影响

	for i := 0; i < b.N; i++ {
		DirectIO(GetTestData())
	}

	b.StopTimer() // 停止计时器，排除清理代码的影响

	// 报告基准测试结果
	b.ReportMetric(float64(b.N), "iterations") // 报告迭代次数
}

func BenchmarkBufferdIO(b *testing.B) {
	b.ResetTimer() // 重置计时器，排除初始化代码的影响

	for i := 0; i < b.N; i++ {
		BufferdIO(GetTestData())
	}

	b.StopTimer() // 停止计时器，排除清理代码的影响

	// 报告基准测试结果
	b.ReportMetric(float64(b.N), "iterations") // 报告迭代次数
}

func DirectIO(testData []byte) {
	// Make a temporary file name
	fd, err := os.CreateTemp("/Users/hlhf/GolandProjects/testdata", "direct_io_test")
	if err != nil {
		log.Fatal("Failed to make temp file", err)
	}
	path := fd.Name()
	fd.Close()

	// 使用直接IO进行测试
	fileDirectIO, err := directio.OpenDirectFile(path, os.O_CREATE|os.O_WRONLY, 0666)
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

func TestDirectIOAndBufferdIOTime(t *testing.T) {
	// 使用缓冲IO进行测试
	testData := GetTestData()
	startTime := time.Now()
	directIOTime := time.Since(startTime)
	DirectIO(testData)
	// 输出执行时间
	fmt.Println("Direct IO time:", directIOTime)

	startTime2 := time.Now()
	bufferdIOTime := time.Since(startTime2)
	BufferdIO(testData)
	// 输出执行时间
	fmt.Println("Buffered IO time:", bufferdIOTime)
}
