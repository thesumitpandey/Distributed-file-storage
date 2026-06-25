package main

import (
	"Distributed-file-storage/p2p"
	"fmt"
	"io"
	"log"
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
	store *Store
	Quit  chan struct{}
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
	}

}

func (f *FileServer) Stop() {
	close(f.Quit)
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

		case msg := <-f.Transport.Consume():
			fmt.Println(msg)

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

	f.Loop()

	return nil
}

func (f *FileServer) Store(key string, r io.Reader) error {
	return f.store.Write(key, r)
}
