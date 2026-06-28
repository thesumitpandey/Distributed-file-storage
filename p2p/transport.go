package p2p

import "net"

type Peer interface {
  net.Conn
  CloseStream()
	Send([]byte)error
}

type Transport interface {
	ListenAndAccept() error
	Consume() <-chan Message
	Close() error
	Dial(string) error
}
