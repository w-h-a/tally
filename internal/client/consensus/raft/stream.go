package raft

import (
	"net"
	"time"

	hraft "github.com/hashicorp/raft"
)

// streamLayer provides the network transport for Raft peer
// communication over plain TCP.
type streamLayer struct {
	ln net.Listener
}

func newStreamLayer(ln net.Listener) *streamLayer {
	return &streamLayer{ln: ln}
}

func (s *streamLayer) Dial(addr hraft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(addr), timeout)
}

func (s *streamLayer) Accept() (net.Conn, error) {
	return s.ln.Accept()
}

func (s *streamLayer) Close() error {
	return s.ln.Close()
}

func (s *streamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
