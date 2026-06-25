package main

import "Distributed-file-storage/p2p"

func main() {

	transPort := p2p.NewTcpTransport("3000")

  fileServerOpts := FileServerOpts{
    Transport: transPort,
    StoreRoot: "root",
		PathTransformFunc: CASPathStorageFunc,
	}

	fileServer:=NewFileServer(fileServerOpts)
  fileServer.Start()

}
