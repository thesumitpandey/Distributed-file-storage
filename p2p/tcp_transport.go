package p2p

import (
	"fmt"
	"net"
	"sync"
)

type TcpPeer struct {
	conn     net.Conn
	outbound bool
}

func NewTcpPeer(conn net.Conn, outbound bool) peer {
	return &TcpPeer{
		conn:     conn,
		outbound: outbound,
	}

}

func (p*TcpPeer) Close() error {
return p.conn.Close()
}

type TcpTransport struct {
	listenAddress string
	listener      net.Listener
	handshake     HandshakeFunc
	decoder       Decoder
	rpcch        chan Message

	mu    sync.RWMutex
	peers map[net.Addr]peer
}

func NewTcpTransport(listenAddress string) *TcpTransport {
	return &TcpTransport{
		handshake:     NOPHandshakeFunc,
		listenAddress: listenAddress,
		decoder: DefaultDecoder{},
	}
}

func (t *TcpTransport) Consume() <-chan Message  {
return t.rpcch
}

func (t *TcpTransport) ListenAndAccept() error {
	var err error

	t.listener, err = net.Listen("tcp", t.listenAddress)
	if err != nil {
		return err
	}

	go t.startAcceptLoop()

	return nil
}

func (t *TcpTransport) startAcceptLoop() {

	for {
		conn, err := t.listener.Accept()
		if err != nil {
			fmt.Printf("error accepting connection %v\n", err)
		}
		go t.handleConn(conn)
	}

}


type temp struct {
  
}

func (t *TcpTransport) handleConn(conn net.Conn) {

	peer := NewTcpPeer(conn, true)

	if err := t.handshake(peer); err != nil {
		fmt.Printf("error during handshake %v\n", err)
		conn.Close()
		return
	}

	msg := &Message{}
	for {
		if err := t.decoder.Decode(conn, msg); err != nil {
			fmt.Printf("error decoding message %v\n", err)
			continue
		}

		msg.From=conn.RemoteAddr()
		t.rpcch <- *msg
		fmt.Printf("received message %s\n", msg)

	}

}
