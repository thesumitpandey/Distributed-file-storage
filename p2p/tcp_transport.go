package p2p

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
)

type TcpPeer struct {
	conn     net.Conn
	outbound bool
}

func NewTcpPeer(conn net.Conn, outbound bool) Peer {
	return &TcpPeer{
		conn:     conn,
		outbound: outbound,
	}

}

func (p *TcpPeer) Close() error {
	return p.conn.Close()
}

type TcpTransport struct {
	listenAddress string
	listener      net.Listener
	handshake     HandshakeFunc
	decoder       Decoder
	rpcch         chan Message

	mu     sync.RWMutex
	peers  map[net.Addr]Peer
	onPeer func(Peer) error
}

func NewTcpTransport(listenAddress string) Transport {
	return &TcpTransport{
		handshake:     NOPHandshakeFunc,
		listenAddress: listenAddress,
		decoder:       DefaultDecoder{},
		rpcch:         make(chan Message),
		onPeer: func(Peer) error {
			fmt.Println("some login")
			return nil
		},
	}
}

func (t *TcpTransport) Dial(addr string) error {
	conn, err := net.Dial("tcp", addr)

	if err != nil {
		return err
	}

	go t.handleConn(conn, true)

	return nil
}

func (t *TcpTransport) Close() error {
	return t.listener.Close()
}

func (t *TcpTransport) Consume() <-chan Message {
	return t.rpcch
}

func (t *TcpTransport) ListenAndAccept() error {
	var err error

	t.listener, err = net.Listen("tcp", t.listenAddress)
	if err != nil {
		return err
	}

	go t.startAcceptLoop()

	log.Printf("listening on %s\n", t.listenAddress)

	return nil
}

func (t *TcpTransport) startAcceptLoop() {

	for {
		conn, err := t.listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}

		if err != nil {
			fmt.Printf("error accepting connection %v\n", err)
		}
		go t.handleConn(conn,false)
	}

}

type temp struct {
}

func (t *TcpTransport) handleConn(conn net.Conn, outbound bool) {
	var er error

	defer func() {
		fmt.Println("error :", er)
		conn.Close()
	}()

	peer := NewTcpPeer(conn, outbound)

	if err := t.handshake(peer); err != nil {
		return
	}

	if t.onPeer != nil {
		if t.onPeer(peer) != nil {
			er = t.onPeer(peer)
			return
		}
	}

	msg := &Message{}
	for {
		if err := t.decoder.Decode(conn, msg); err != nil {
			fmt.Printf("error decoding message %v\n", err)
			continue
		}

		msg.From = conn.RemoteAddr()
		t.rpcch <- *msg
	}

}
