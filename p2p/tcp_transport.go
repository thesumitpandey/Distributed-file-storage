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
	Wg     sync.WaitGroup
}

func NewTcpPeer(conn net.Conn, outbound bool) *TcpPeer {
	return &TcpPeer{
		conn:     conn,
		outbound: outbound,
	}

}

func (p *TcpPeer) Send(msg []byte) error {
	_, err := p.conn.Write(msg)
	if err != nil {
		return err
	}
	return nil
}

func (p *TcpPeer) Read(data []byte) (int, error){
	return p.conn.Read(data)
}

func (p *TcpPeer) Close() error {
	return p.conn.Close()
}

func (p *TcpPeer) RemoteAddr() net.Addr {
	return p.conn.RemoteAddr()
}

type TcpTransport struct {
	listenAddress string
	listener      net.Listener
	handshake     HandshakeFunc
	decoder       Decoder
	rpcch         chan Message

	mu     sync.RWMutex
	peers  map[net.Addr]Peer
	OnPeer func(Peer) error
}

func NewTcpTransport(listenAddress string) *TcpTransport {
	return &TcpTransport{
		handshake:     NOPHandshakeFunc,
		listenAddress: listenAddress,
		decoder:       DefaultDecoder{},
		rpcch:         make(chan Message),
		OnPeer: func(Peer) error {
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
		go t.handleConn(conn, false)
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

	if t.OnPeer != nil {
		if t.OnPeer(peer) != nil {
			return
		}
	}

	msg := &Message{}
	for {
		if err := t.decoder.Decode(conn, msg); err != nil {
			fmt.Printf("error decoding message %v\n", err)
			continue
		}

		msg.From = conn.RemoteAddr().String()

		fmt.Println("message recieved in transport",msg.Payload)
		peer.Wg.Add(1)
		t.rpcch <- *msg
		peer.Wg.Wait()

	}

}
