package partition

import (
	"errors"
	"github.com/Mulily0513/C2KV/db/iooperator"
	"github.com/google/uuid"
	"io"
	"os"
)

//todo 1、一个value如果跨两个block，那么可能需要访问两次硬盘,后续优化vlog中数据的对齐

type SST struct {
	Id uint64
	fd *os.File
	fp string
}

func OpenSST(filePath string) (*SST, error) {
	return &SST{
		Id: uint64(uuid.New().ID()),
		fd: iooperator.OpenBufferIOFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666),
		fp: filePath,
	}, nil
}

func (s *SST) Write(buf []byte) (err error) {
	if _, err = s.fd.Write(buf); err != nil {
		return err
	}
	return
}

func (s *SST) Read(vSize, vOffset int64) (buf []byte, err error) {
	if _, err = s.fd.Seek(vOffset, io.SeekStart); err != nil {
		return nil, err
	}
	buf = make([]byte, vSize)
	if _, err = s.fd.Read(buf); err != nil {
		return nil, err
	}
	return
}

func (s *SST) Close() (err error) {
	if s.fd == nil {
		return errors.New("fd is not exist")
	}
	return s.fd.Close()
}

func (s *SST) Remove() (err error) {
	if s.fd == nil {
		return errors.New("fd is not exist")
	}
	return os.Remove(s.fp)
}

func (s *SST) Rename(fp string) (err error) {
	if err = s.fd.Close(); err != nil {
		return
	}
	if err = os.Rename(s.fp, fp); err != nil {
		return
	}
	s.fd = iooperator.OpenBufferIOFile(fp, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	s.fp = fp
	return err
}
