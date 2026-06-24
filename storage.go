package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
)

func CASPathStorageFunc(path string) PathKey {
	hash := sha1.Sum([]byte(path))
	hashStr := hex.EncodeToString(hash[:])

	blockSize := 5
	slicelen := len(hashStr) / blockSize

	paths := make([]string, slicelen)

	for i := 0; i < slicelen; i++ {
		paths[i] = hashStr[i*blockSize : (i+1)*blockSize]
	}

	return PathKey{
		PathName: strings.Join(paths, "/"),
		fileName: hashStr,
	}

}

type PathKey struct {
	PathName string
	fileName string
}

func (p *PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.PathName, p.fileName)
}

func (p *PathKey) FirstPath() string {	
  firstPath:=strings.Split(p.PathName, "/")[0]
	return firstPath
}

type PathTransformFunc func(string) PathKey

type StoreOpts struct {
	PathTransformFunc PathTransformFunc
}

type Store struct {
	StoreOpts
}

func NewStore(opts StoreOpts) *Store {
	return &Store{StoreOpts: opts}
}


func(s * Store) Delete(key string)error{
	pathKey:=s.PathTransformFunc(key)
  
	return os.RemoveAll(pathKey.FirstPath())
}

func (s *Store) Read(key string) (io.Reader, error) {
	f, err := s.readStream(key)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, f)

	return buf,err
}

func (s *Store) readStream(key string) (io.ReadCloser, error) {
	pathKey := s.PathTransformFunc(key)
	return os.Open(pathKey.FullPath())
}

func (s *Store) WriteStrem(key string, r io.Reader) error {

	pathKey := s.PathTransformFunc(key)
	if err := os.MkdirAll(pathKey.PathName, os.ModePerm); err != nil {
		return err
	}

	filepath := pathKey.FullPath()

	f, err := os.Create(filepath)
	if err != nil {
		return err
	}

	defer f.Close()

	n, err := io.Copy(f, r)
	if err != nil {
		return err
	}

	fmt.Println("wrote", n, "bytes to", filepath)

	return nil
}
