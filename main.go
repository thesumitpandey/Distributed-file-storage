package main

import (
	"Distributed-file-storage/p2p"
	"bytes"
	"fmt"
	"time"
)

func makeServer(addr string, nodes ...string) *FileServer {
	transPort := p2p.NewTcpTransport(addr)

	fileServerOpts := FileServerOpts{
		Transport:         transPort,
		StoreRoot:         "root",
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

	time.Sleep(2*time.Second)

go	s2.Start()

	time.Sleep(2 * time.Second)

	b:=[]byte("check")

	fmt.Println("check")
	err:=s2.StoreMessage("checkkey",bytes.NewReader(b))
if err!=nil{
	fmt.Println(err)
}


// for{}

}
