package transport

import (
	"encoding/binary"
	"errors"
	"github.com/Mulily0513/C2KV/src/log"
	"github.com/Mulily0513/C2KV/src/pb"
	"io"
	"net"
)

type msgEncoderAndWriter struct {
	w net.Conn
}

func (enc *msgEncoderAndWriter) encodeAndWrite(m *pb.Message) (int, error) {
	b, err := m.Marshal()
	if err != nil {
		log.Panicf("marshal failed:%v", err)
	}
	if enc.w == nil {
		return 0, errors.New("conn is nil")
	}
	return enc.w.Write(newPackage(DefaultId, b).marshal())
}

type msgDecoderAndReader struct {
	r net.Conn
}

func (dec *msgDecoderAndReader) decodeAndRead() (*pb.Message, error) {
	m := new(pb.Message)
	pkg, err := dec.unmarshalPkg()
	if err != nil && err != io.EOF {
		return nil, err
	}
	if err = m.Unmarshal(pkg.Data); err != nil {
		log.Panicf("unmarshal failed:%v", err)
	}
	return m, nil
}

func (dec *msgDecoderAndReader) unmarshalPkg() (pkg Package, err error) {
	headData := make([]byte, HeaderLength)
	if _, err = dec.r.Read(headData); err != nil {
		return pkg, err
	}
	pkg.Id = headData[zero]
	pkg.DataLen = binary.LittleEndian.Uint32(headData[IDLength:HeaderLength])
	pkg.Data = make([]byte, pkg.DataLen)
	if _, err = dec.r.Read(pkg.Data); err != nil {
		return
	}
	return
}

const (
	HeaderLength = 5 // HeaderLength id + dataLen
	IDLength     = 1
	zero         = 0
	DefaultId    = 1
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
