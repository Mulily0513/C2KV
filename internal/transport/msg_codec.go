package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Mulily0513/C2KV/api/gen/pb"
	"io"
	"net"
	"sync"
)

type msgEncoderAndWriter struct {
	w net.Conn
}

var encodeBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256*1024)
		return &b
	},
}

func (enc *msgEncoderAndWriter) encodeAndWrite(m *pb.Message) (int, error) {
	return enc.encodeAndWriteBatch([]*pb.Message{m})
}

func (enc *msgEncoderAndWriter) encodeAndWriteBatch(msgs []*pb.Message) (int, error) {
	if len(msgs) == 0 {
		return 0, nil
	}
	if enc.w == nil {
		return 0, errors.New("conn is nil")
	}
	estimate := 0
	sizes := make([]int, len(msgs))
	for i, m := range msgs {
		if m == nil {
			return 0, errors.New("message is nil")
		}
		size := m.Size()
		sizes[i] = size
		estimate += HeaderLength + size
	}

	poolItem := encodeBufPool.Get().(*[]byte)
	buf := (*poolItem)[:0]
	if cap(buf) < estimate {
		buf = make([]byte, 0, estimate)
	}
	for i, m := range msgs {
		var err error
		buf, err = appendMarshaledPackage(buf, m, sizes[i])
		if err != nil {
			*poolItem = buf[:0]
			encodeBufPool.Put(poolItem)
			return 0, fmt.Errorf("marshal message failed: %w", err)
		}
	}
	n, err := writeAll(enc.w, buf)
	*poolItem = buf[:0]
	encodeBufPool.Put(poolItem)
	if err != nil {
		return n, fmt.Errorf("write package failed: %w", err)
	}
	return n, nil
}

func appendMarshaledPackage(dst []byte, m *pb.Message, payloadSize int) ([]byte, error) {
	if m == nil {
		return dst, errors.New("message is nil")
	}

	start := len(dst)
	dst = append(dst, make([]byte, HeaderLength+payloadSize)...)
	dst[start+zero] = DefaultId
	binary.LittleEndian.PutUint32(dst[start+IDLength:start+HeaderLength], uint32(payloadSize))

	payload := dst[start+HeaderLength : start+HeaderLength+payloadSize]
	n, err := m.MarshalToSizedBuffer(payload)
	if err != nil {
		return nil, err
	}
	if n != payloadSize {
		return nil, fmt.Errorf("marshal size mismatch, want=%d got=%d", payloadSize, n)
	}
	return dst, nil
}

type msgDecoderAndReader struct {
	r net.Conn
}

func (dec *msgDecoderAndReader) decodeAndRead() (*pb.Message, error) {
	m := new(pb.Message)
	pkg, err := dec.unmarshalPkg()
	if err != nil {
		return nil, err
	}
	if err = m.Unmarshal(pkg.Data); err != nil {
		return nil, fmt.Errorf("unmarshal message failed: %w", err)
	}
	return m, nil
}

func (dec *msgDecoderAndReader) unmarshalPkg() (pkg Package, err error) {
	if dec.r == nil {
		return pkg, errors.New("conn is nil")
	}
	var headData [HeaderLength]byte
	if _, err = io.ReadFull(dec.r, headData[:]); err != nil {
		return pkg, err
	}
	pkg.Id = headData[zero]
	pkg.DataLen = binary.LittleEndian.Uint32(headData[IDLength:HeaderLength])
	if pkg.DataLen == 0 {
		return pkg, errors.New("invalid package: empty payload")
	}
	if pkg.DataLen > MaxPackageDataLen {
		return pkg, fmt.Errorf("invalid package: payload too large %d", pkg.DataLen)
	}
	pkg.Data = make([]byte, pkg.DataLen)
	if _, err = io.ReadFull(dec.r, pkg.Data); err != nil {
		return
	}
	return
}

const (
	HeaderLength      = 5 // HeaderLength id + dataLen
	IDLength          = 1
	zero              = 0
	DefaultId         = 1
	MaxPackageDataLen = 64 << 20
)

type Package struct {
	Id      uint8
	DataLen uint32
	Data    []byte
}

func newPackage(id uint8, data []byte) *Package {
	return &Package{
		DataLen: uint32(len(data)),
		Id:      id,
		Data:    data,
	}
}

func (pkg *Package) marshal() []byte {
	buf := make([]byte, HeaderLength+pkg.DataLen)
	buf[zero] = pkg.Id
	binary.LittleEndian.PutUint32(buf[IDLength:HeaderLength], pkg.DataLen)
	copy(buf[HeaderLength:], pkg.Data)
	return buf
}

func writeAll(w net.Conn, data []byte) (int, error) {
	total := 0
	for total < len(data) {
		n, err := w.Write(data[total:])
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}
