package gocouchbaseio

import (
	"crypto/tls"
	"io"
	"net"
)

type DefaultDialer struct {
}

func (d *DefaultDialer) Dial(address string, useSsl bool) (io.ReadWriteCloser, error) {
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}

	conn.SetNoDelay(false)

	if !useSsl {
		return conn, nil
	}

	tlsConn := tls.Client(conn, &tls.Config{
		InsecureSkipVerify: true,
	})
	if err != nil {
		return nil, err
	}

	return tlsConn, nil
}
