package main

import (
	"Distributed-file-storage/p2p"
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"
)

func makeServer(addr string, nodes ...string) *FileServer {
	transPort := p2p.NewTcpTransport(addr)

	fileServerOpts := FileServerOpts{
		Transport:         transPort,
		StoreRoot:         strings.TrimPrefix(addr, ":") + "_network",
		PathTransformFunc: CASPathStorageFunc,
		BootStrapNodes:    nodes,
	}

	fileServer := NewFileServer(fileServerOpts)
	transPort.OnPeer = fileServer.OnPeer

	return fileServer
}

func main() {

	s1 := makeServer(":3000")
	s2 := makeServer(":4000", ":3000")

	go func() { s1.Start() }()

	time.Sleep(2 * time.Second)

	go s2.Start()

	time.Sleep(2 * time.Second)

	// b := []byte("maindata")

	// for i := 0; i < 10; i++ {
	// 	err := s2.Store(fmt.Sprintf("%d_mainkey%d", i, i), bytes.NewReader(b))
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}
	// }

	r, _ := s2.Get("1_mainkey1")

	data := new(bytes.Buffer)
	io.Copy(data, r)
	fmt.Println(data.String())

	for {
	}

}
