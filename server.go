package main

import (
	"Distributed-file-storage/p2p"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"
)

type FileServerOpts struct {
	PathTransformFunc PathTransformFunc
	StoreRoot         string
	ListenAddress     string
	Transport         p2p.Transport
	BootStrapNodes    []string
}

type FileServer struct {
	FileServerOpts
	store    *Store
	Quit     chan struct{}
	Peers    map[string]p2p.Peer
	PeerLock sync.Mutex
}

func NewFileServer(opts FileServerOpts) *FileServer {
	storeOpts := StoreOpts{
		Root:              opts.StoreRoot,
		PathTransformFunc: opts.PathTransformFunc,
	}

	return &FileServer{
		FileServerOpts: opts,
		store:          NewStore(storeOpts),
		Quit:           make(chan struct{}),
		Peers:          make(map[string]p2p.Peer),
	}

}

func (f *FileServer) Stop() {
	close(f.Quit)
}

func (f *FileServer) OnPeer(p p2p.Peer) error {
	f.PeerLock.Lock()
	defer f.PeerLock.Unlock()

	f.Peers[p.RemoteAddr().String()] = p

	log.Println("connected to peer", p.RemoteAddr().String())

	return nil

}

type Message struct {
	Payload any
}

type MessageStoreFile struct {
	Key  string
	Size int64
}

type MessageGetStoreFile struct {
	Key string
}

func (f *FileServer) Get(key string) (io.Reader, error) {
	if f.store.Has(key) {
		fmt.Printf("serving file (%s) from local disk\n", key)

		_, r, err := f.store.Read(key)
		if err != nil {
			return nil, err
		}
		return r, nil

	}

	fmt.Printf("dont have file (%s) locally, fetching from network...\n", key)
	msg := Message{
		Payload: MessageGetStoreFile{
			Key: key,
		},
	}

	err := f.broadcast(msg)
	if err != nil {
		return nil, err
	}

	for _, peer := range f.Peers {

		var fileSize int64
		binary.Read(peer, binary.LittleEndian, &fileSize)

		n, err := f.store.Write(key, io.LimitReader(peer, fileSize))
		if err != nil {
			return nil, err
		}

		fmt.Printf("recieve %d byte over the network from %s", n, peer.RemoteAddr().String())


		peer.CloseStream()
	}

	_, r, err := f.store.Read(key)
		if err != nil {
			return nil, err
		}
		return r, nil
}

func (f *FileServer) stream(msg Message, fileBuffer *bytes.Buffer) error {
	peers := []io.Writer{}
	for _, p := range f.Peers {
		peers = append(peers, p)
	}

	// encode to buffer first
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	// then multiwrite complete bytes
	mw := io.MultiWriter(peers...)
	_, err := mw.Write([]byte{p2p.IncomingMessage})
	if err != nil {
		return err
	}

	_, err = mw.Write(buf.Bytes())
	if err != nil {
		return err
	}

	time.Sleep(time.Millisecond * 500)

	_, err = mw.Write([]byte{p2p.IncomingStream})

	_, err = mw.Write(fileBuffer.Bytes())
	return err

}

func (f *FileServer) broadcast(msg Message) error {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	for _, peer := range f.Peers {
		fmt.Println("sending to:", peer.RemoteAddr().String())
		peer.Send([]byte{p2p.IncomingMessage})
		if err := peer.Send(buf.Bytes()); err != nil {
			return err
		}
	}

	return nil

}

func (f *FileServer) Store(key string, r io.Reader) error {

	var (
		fileBuffer = new(bytes.Buffer)
		tee        = io.TeeReader(r, fileBuffer)
	)

	//store in current system
	size, err := f.store.Write(key, tee)
	if err != nil {
		return err
	}

	msg := Message{
		Payload: MessageStoreFile{
			Key:  key,
			Size: size,
		},
	}

	err = f.stream(msg, fileBuffer)
	if err != nil {
		return err
	}
	return nil
}

func (f *FileServer) BootStrapNetwork() error {

	for _, addr := range f.BootStrapNodes {
		go func(addr string) {
			fmt.Println("attempting to connect", addr)

			if err := f.Transport.Dial(addr); err != nil {
				log.Println("dial error:", err)
				return
			}

		}(addr)

	}
	return nil
}

func (f *FileServer) Loop() {

	for {

		select {

		case rpc := <-f.Transport.Consume():
			fmt.Println("rcv")
			var msg Message
			if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&msg); err != nil {
				log.Println(err)
				fmt.Println("error from here")
				return
			}

			fmt.Printf("msg payload: %+v\n", &msg)

			f.handleMessage(rpc.From, &msg)

		case <-f.Quit:
			return

		}

	}
}

func (f *FileServer) handleMessage(from string, msg *Message) error {
	switch v := msg.Payload.(type) {
	case MessageStoreFile:
		f.handleMessageStoreFile(from, v)

	case MessageGetStoreFile:
		f.handleMessageGetStoreFile(from, v)
	}

	return nil
}

func (f *FileServer) handleMessageStoreFile(from string, msg MessageStoreFile) error {
	peer, ok := f.Peers[from]
	if !ok {
		return fmt.Errorf("peer not found in peer list")
	}

	if _, err := f.store.Write(msg.Key, io.LimitReader(peer, msg.Size)); err != nil {
		return err
	}

	peer.CloseStream()

	return nil

}

func (f *FileServer) handleMessageGetStoreFile(from string, msg MessageGetStoreFile) error {
	peer, ok := f.Peers[from]
	if !ok {
		return fmt.Errorf("peer not found in peer list")
	}

	if !f.store.Has(msg.Key) {
		return fmt.Errorf("file %s doesnot exist on disk", msg.Key)
	}

	n, r, err := f.store.Read(msg.Key)

	peer.Send([]byte{p2p.IncomingStream})

	binary.Write(peer, binary.LittleEndian, int64(n))

	_, err = io.CopyN(peer, r, n)
	if err != nil {
		return err
	}

	fmt.Printf("writen %d bytes to peer %s\n", n, from)

	return nil

}

func (f *FileServer) Start() error {
	defer func() {
		fmt.Println("server is stopped")
	}()

	if err := f.Transport.ListenAndAccept(); err != nil {
		return err
	}

	f.BootStrapNetwork()

	f.Loop()

	return nil
}

func (f *FileServer) StoreData(key string, r io.Reader) (int64, error) {
	return f.store.Write(key, r)
}

func init() {
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetStoreFile{})
}
