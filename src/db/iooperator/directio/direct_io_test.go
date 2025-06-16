package directio

import (
	"bytes"
	"fmt"
	"github.com/ncw/directio"
	"io"
	"os"
	"testing"
	"time"
)

var path = "/Users/hlhf/GolandProjects/directio/direct_io_test"
var path1 = "/Users/hlhf/GolandProjects/directio/buffer_io_test"

const (
	block = 1
	nums  = 1
)

func TestDirectIO(t *testing.T) {
	// starting block
	block1 := make([]byte, block*nums)
	for i := 0; i < len(block1); i++ {
		block1[i] = 'A'
	}
	block2 := make([]byte, block*nums)

	timeNow1 := time.Now()
	WriteDirectIO(block1)
	fmt.Println("write Direct IO time:", time.Since(timeNow1))
	timeNow2 := time.Now()
	ReadDirectIO(block2)
	fmt.Println("read Direct IO time:", time.Since(timeNow2))

	// Tidy
	err := os.Remove(path)
	if err != nil {
		t.Fatal("Failed to remove temp file", path, err)
	}

	// Compare
	if !bytes.Equal(block1, block2) {
		t.Fatal("Read not the same as written")
	}
}

func WriteDirectIO(block1 []byte) {
	// Write the file
	out, err := directio.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Println(err)
	}
	_, err = out.Write(block1)
	if err != nil {
		fmt.Println(err)
	}

}

func ReadDirectIO(block2 []byte) {
	in, err := directio.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		fmt.Println(err)
	}

	_, err = in.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	_, err = in.Read(block2)
	if err != nil {
		fmt.Println(err)
	}
	err = in.Close()
	if err != nil {
		fmt.Println(err)
	}
}

func TestBufferdIO(t *testing.T) {
	// starting block
	block1 := make([]byte, block*nums)
	for i := 0; i < len(block1); i++ {
		block1[i] = 'A'
	}
	block2 := make([]byte, block*nums)

	timeNow1 := time.Now()
	WriteBufferIO(block1)
	fmt.Println("write buffer IO time:", time.Since(timeNow1))
	timeNow2 := time.Now()
	ReadBufferIO(block2)
	fmt.Println("read buffer IO time:", time.Since(timeNow2))

	// Tidy
	err := os.Remove(path1)
	if err != nil {
		t.Fatal("Failed to remove temp file", path, err)
	}

	// Compare
	if !bytes.Equal(block1, block2) {
		t.Fatal("Read not the same as written")
	}
}

func WriteBufferIO(block1 []byte) {
	fileBufferedIO, err := os.OpenFile(path1, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Println(err)
	}
	fileBufferedIO.Write(block1)
	fileBufferedIO.Sync()
	fileBufferedIO.Close()

}

func ReadBufferIO(block2 []byte) {
	in, err := os.OpenFile(path1, os.O_RDONLY, 0666)
	if err != nil {
		fmt.Println(err)
	}

	_, err = in.Seek(0, io.SeekStart)
	if err != nil {
		return
	}

	_, err = in.Read(block2)
	if err != nil {
		fmt.Println(err)
	}
	err = in.Close()
	if err != nil {
		fmt.Println(err)
	}
}

func TestDirectIOAndBufferIO(t *testing.T) {
	TestDirectIO(t)
	TestBufferdIO(t)
}
