package p2p

import "net"

type Peer interface {
	RemoteAddr() net.Addr
	Close() error
	Send(b []byte) error
	Read(b []byte) (int, error)
}

type Transport interface {
	ListenAndAccept() error
	Consume() <-chan Message
	Close() error
	Dial(string) error
}
