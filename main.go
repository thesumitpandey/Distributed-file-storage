package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
)

func main() {
	opts := StoreOpts{
		PathTransformFunc: CASPathStorageFunc,
	}

	s := NewStore(opts)

	key := "qwertyuiop"
	data := []byte("hello world")

	if err := s.WriteStrem(key, bytes.NewReader(data)); err != nil {
		fmt.Println(err)
	}

	r, err := s.Read(key)
	if err != nil {
		log.Fatal(err)
	}

	b, _ := io.ReadAll(r)

	fmt.Println(string(b))

	if err:=s.Delete(key); err != nil {
		fmt.Println(err)
	}
	
}
