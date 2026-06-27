package main

import (
	"Distributed-file-storage/p2p"
	"bytes"
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

type MessageStoreFile struct{
	Key string
}

func (f *FileServer) StoreMessage(key string, r io.Reader) error {

	buf := new(bytes.Buffer)

	msg:=Message{
		Payload: MessageStoreFile{
			Key: key,
		},
	}

	if err:=gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

  for _, peer := range f.Peers {
		fmt.Println("sending to:",peer.RemoteAddr().String())
    if err:=peer.Send(buf.Bytes()) ; err != nil {
      return err
    }
  }


time.Sleep(4*time.Second);


	return nil;

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
		 log.Println("rcv")
		 var msg Message
		 if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&msg); err != nil {
		  log.Println(err) 
			return 
		 }
		
		 fmt.Printf("msg payload: %+v\n", msg)

    p,ok:=f.Peers[rpc.From]
    if !ok {
      panic("Peer not found in peer map")
    }

		// b:=make([]byte, 1024)

		// if _,err:=p.Read(b); err != nil {
		// 	panic(err)
		// }

    // fmt.Println(string(b))

		p.(*p2p.TcpPeer).Wg.Done()

		case <-f.Quit:
			return

		}

	}
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

func (f *FileServer) Store(key string, r io.Reader) error {
	return f.store.Write(key, r)
}

func init(){
	gob.Register(MessageStoreFile{})
}