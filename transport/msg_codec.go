package transport

import (
	"encoding/binary"
	"github.com/ColdToo/Cold2DB/pb"
	"net"
)

type msgEncoderAndWriter struct {
	w net.Conn
}

func (enc *msgEncoderAndWriter) encodeAndWrite(m pb.Message) error {
	b, _ := m.Marshal()
	_, err := enc.w.Write(newPackage(DefaultId, b).marshal())
	if err != nil {
		return err
	}
	return nil
}

type msgDecoderAndReader struct {
	r net.Conn
}

func (dec *msgDecoderAndReader) decodeAndRead() (pb.Message, error) {
	var m pb.Message
	pkg, err := unmarshalPkg(dec.r)
	if err != nil {
		return pb.Message{}, err
	}
	return m, m.Unmarshal(pkg.Data)
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

func unmarshalPkg(rc net.Conn) (pkg Package, err error) {
	headData := make([]byte, HeaderLength)
	if _, err = rc.Read(headData); err != nil {
		return pkg, err
	}
	pkg.Id = headData[zero]
	pkg.DataLen = binary.LittleEndian.Uint32(headData[IDLength:HeaderLength])
	pkg.Data = make([]byte, pkg.DataLen)

	if _, err = rc.Read(pkg.Data); err != nil {
		return
	}
	return
}
