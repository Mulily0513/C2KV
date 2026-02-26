package partition

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/google/uuid"
	"hash/fnv"
	"io"
	"os"
)

//todo 1. If a value spans two blocks, it may need to access the hard disk twice.
//Later, optimize the alignment of data in the vlog

type SST struct {
	Id         uint64
	fd         *os.File
	fp         string
	dataOffset int64
}

const (
	sstHeaderMagic    = "C2SV"
	sstHeaderMagicLen = 4
	sstHeaderSize     = sstHeaderMagicLen + 8
)

func OpenSST(filePath string) (*SST, error) {
	fd, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	id, dataOffset, err := loadOrInitSSTHeader(fd, filePath)
	if err != nil {
		_ = fd.Close()
		return nil, err
	}
	if _, err = fd.Seek(0, io.SeekEnd); err != nil {
		_ = fd.Close()
		return nil, err
	}
	return &SST{
		Id:         id,
		fd:         fd,
		fp:         filePath,
		dataOffset: dataOffset,
	}, nil
}

func (s *SST) Write(buf []byte) (err error) {
	if _, err = s.fd.Write(buf); err != nil {
		return err
	}
	return
}

func (s *SST) Read(vSize, vOffset int64) (buf []byte, err error) {
	buf = make([]byte, vSize)
	n, err := s.fd.ReadAt(buf, s.dataOffset+vOffset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if int64(n) != vSize {
		return nil, io.ErrUnexpectedEOF
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
	s.fd, err = os.OpenFile(fp, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	s.Id, s.dataOffset, err = loadOrInitSSTHeader(s.fd, fp)
	if err != nil {
		return err
	}
	if _, err = s.fd.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	s.fp = fp
	return err
}

func loadOrInitSSTHeader(fd *os.File, filePath string) (uint64, int64, error) {
	if fd == nil {
		return 0, 0, os.ErrInvalid
	}
	info, err := fd.Stat()
	if err != nil {
		return 0, 0, err
	}
	if info.Size() == 0 {
		id := uint64(uuid.New().ID())
		header := make([]byte, sstHeaderSize)
		copy(header[:sstHeaderMagicLen], []byte(sstHeaderMagic))
		binary.LittleEndian.PutUint64(header[sstHeaderMagicLen:], id)
		if n, writeErr := fd.WriteAt(header, 0); writeErr != nil {
			return 0, 0, writeErr
		} else if n != len(header) {
			return 0, 0, io.ErrShortWrite
		}
		return id, sstHeaderSize, nil
	}
	if info.Size() >= sstHeaderSize {
		header := make([]byte, sstHeaderSize)
		n, readErr := fd.ReadAt(header, 0)
		if readErr != nil && readErr != io.EOF {
			return 0, 0, readErr
		}
		if n == sstHeaderSize && bytes.Equal(header[:sstHeaderMagicLen], []byte(sstHeaderMagic)) {
			id := binary.LittleEndian.Uint64(header[sstHeaderMagicLen:])
			if id != 0 {
				return id, sstHeaderSize, nil
			}
		}
	}
	// Legacy SST without header. Keep reading from offset 0, and use stable
	// hash-based ID to avoid per-open randomness.
	return legacySSTID(filePath), 0, nil
}

func legacySSTID(filePath string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(filePath))
	id := h.Sum64()
	if id == 0 {
		return 1
	}
	return id
}
