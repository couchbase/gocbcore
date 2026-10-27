//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package gocbcore

import (
	"context"
	"net"
	"syscall"
	"testing"
	"time"
)

func TestDialMemdConnTCPNoDelay(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(tcpNoDelayTestName(enabled), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()

			accepted := make(chan net.Conn, 1)
			go func() {
				connection, acceptErr := listener.Accept()
				if acceptErr == nil {
					accepted <- connection
				}
				close(accepted)
			}()

			connection, err := dialMemdConn(
				context.Background(),
				routeEndpoint{Address: listener.Addr().String()},
				nil,
				time.Now().Add(time.Second),
				0,
				enabled,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()

			serverConnection := <-accepted
			if serverConnection == nil {
				t.Fatal("listener did not accept the test connection")
			}
			defer serverConnection.Close()

			wrapped, ok := connection.(*memdConnWrap)
			if !ok {
				t.Fatalf("connection = %T, want *memdConnWrap", connection)
			}
			tcpConnection, ok := wrapped.baseConn.Closer.(*net.TCPConn)
			if !ok {
				t.Fatalf("base connection = %T, want *net.TCPConn", wrapped.baseConn.Closer)
			}

			value, err := tcpNoDelayValue(tcpConnection)
			if err != nil {
				t.Fatal(err)
			}
			if got := value != 0; got != enabled {
				t.Fatalf("TCP_NODELAY = %t, want %t", got, enabled)
			}
		})
	}
}

func tcpNoDelayValue(connection *net.TCPConn) (int, error) {
	rawConnection, err := connection.SyscallConn()
	if err != nil {
		return 0, err
	}

	var value int
	var socketErr error
	if err := rawConnection.Control(func(fd uintptr) {
		value, socketErr = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY)
	}); err != nil {
		return 0, err
	}
	return value, socketErr
}

func tcpNoDelayTestName(enabled bool) string {
	if enabled {
		return "enabled"
	}
	return "disabled"
}
