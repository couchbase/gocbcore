package gocbcore

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"time"
)

type memdPacket struct {
	Magic    CommandMagic
	Opcode   CommandCode
	Datatype uint8
	Status   StatusCode
	Vbucket  uint16
	Opaque   uint32
	Cas      uint64
	Key      []byte
	Extras   []byte
	Value    []byte
}

type memdConn interface {
	LocalAddr() string
	RemoteAddr() string
	WritePacket(*memdPacket) error
	ReadPacket(*memdPacket) error
	Close() error
}

type memdTcpConn struct {
	conn       io.ReadWriteCloser
	reader     *bufio.Reader
	headerBuf  []byte
	localAddr  string
	remoteAddr string
}

func DialMemdConn(address string, tlsConfig *tls.Config, deadline time.Time) (memdConn, error) {
	d := net.Dialer{
		Deadline: deadline,
	}

	baseConn, err := d.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	tcpConn := baseConn.(*net.TCPConn)
	tcpConn.SetNoDelay(false)

	var conn io.ReadWriteCloser
	if tlsConfig == nil {
		conn = tcpConn
	} else {
		tlsConn := tls.Client(tcpConn, tlsConfig)
		err = tlsConn.Handshake()
		if err != nil {
			return nil, err
		}

		conn = tlsConn
	}

	return &memdTcpConn{
		conn:       conn,
		reader:     bufio.NewReader(conn),
		headerBuf:  make([]byte, 24),
		localAddr:  baseConn.LocalAddr().String(),
		remoteAddr: address,
	}, nil
}

func (s *memdTcpConn) LocalAddr() string {
	return s.localAddr
}

func (s *memdTcpConn) RemoteAddr() string {
	return s.remoteAddr
}

func (s *memdTcpConn) Close() error {
	return s.conn.Close()
}

func (s *memdTcpConn) WritePacket(req *memdPacket) error {
	extLen := len(req.Extras)
	keyLen := len(req.Key)
	valLen := len(req.Value)

	// Go appears to do some clever things in regards to writing data
	//   to the kernel for network dispatch.  Having a write buffer
	//   per-server that is re-used actually hinders performance...
	// For now, we will simply create a new buffer and let it be GC'd.
	buffer := make([]byte, 24+keyLen+extLen+valLen)

	buffer[0] = uint8(req.Magic)
	buffer[1] = uint8(req.Opcode)
	binary.BigEndian.PutUint16(buffer[2:], uint16(keyLen))
	buffer[4] = byte(extLen)
	buffer[5] = req.Datatype
	if req.Magic != ResMagic {
		binary.BigEndian.PutUint16(buffer[6:], uint16(req.Vbucket))
	} else {
		binary.BigEndian.PutUint16(buffer[6:], uint16(req.Status))
	}
	binary.BigEndian.PutUint32(buffer[8:], uint32(len(buffer)-24))
	binary.BigEndian.PutUint32(buffer[12:], req.Opaque)
	binary.BigEndian.PutUint64(buffer[16:], req.Cas)

	copy(buffer[24:], req.Extras)
	copy(buffer[24+extLen:], req.Key)
	copy(buffer[24+extLen+keyLen:], req.Value)

	_, err := s.conn.Write(buffer)
	return err
}

func (s *memdTcpConn) readFullBuffer(buf []byte) error {
	for len(buf) > 0 {
		r, err := s.reader.Read(buf)
		if err != nil {
			return err
		}

		if r >= len(buf) {
			break
		}

		buf = buf[r:]
	}

	return nil
}

func (s *memdTcpConn) ReadPacket(resp *memdPacket) error {
	err := s.readFullBuffer(s.headerBuf)
	if err != nil {
		return err
	}

	bodyLen := int(binary.BigEndian.Uint32(s.headerBuf[8:]))

	bodyBuf := make([]byte, bodyLen)
	err = s.readFullBuffer(bodyBuf)
	if err != nil {
		return err
	}

	keyLen := int(binary.BigEndian.Uint16(s.headerBuf[2:]))
	extLen := int(s.headerBuf[4])

	resp.Magic = CommandMagic(s.headerBuf[0])
	resp.Opcode = CommandCode(s.headerBuf[1])
	resp.Datatype = s.headerBuf[5]
	if resp.Magic == ResMagic {
		resp.Status = StatusCode(binary.BigEndian.Uint16(s.headerBuf[6:]))
	} else {
		resp.Vbucket = binary.BigEndian.Uint16(s.headerBuf[6:])
	}
	resp.Opaque = binary.BigEndian.Uint32(s.headerBuf[12:])
	resp.Cas = binary.BigEndian.Uint64(s.headerBuf[16:])
	resp.Extras = bodyBuf[:extLen]
	resp.Key = bodyBuf[extLen : extLen+keyLen]
	resp.Value = bodyBuf[extLen+keyLen:]
	return nil
}
