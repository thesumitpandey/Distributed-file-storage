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

var DefaulRoot string = "root"

type PathKey struct {
	PathName string
	fileName string
}

func (p *PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.PathName, p.fileName)
}

func (p *PathKey) FirstPath() string {
	firstPath := strings.Split(fmt.Sprintf(p.PathName), "/")[0]
	return firstPath
}

type PathTransformFunc func(string) PathKey

type StoreOpts struct {
	PathTransformFunc PathTransformFunc
	Root              string
}

type Store struct {
	StoreOpts
}

func NewStore(opts StoreOpts) *Store {
	return &Store{StoreOpts: opts}
}

func (s *Store) Delete(key string) error {
	pathKey := s.PathTransformFunc(key)

	return os.RemoveAll(s.Root + "/" + pathKey.FirstPath())
}

func (s *Store) Read(key string) (io.Reader, error) {
	f, err := s.readStream(key)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, f)

	return buf, err
}

func (s *Store) readStream(key string) (io.ReadCloser, error) {
	pathKey := s.PathTransformFunc(key)
	filepathWithRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.FullPath())

	return os.Open(filepathWithRoot)
}

func (s *Store) Write(key string, r io.Reader) error {
	return s.writeStrem(key, r)
}

func (s *Store) writeStrem(key string, r io.Reader) error {

	if len(s.Root) == 0 {
		s.Root = DefaulRoot
	}

	pathKey := s.PathTransformFunc(key)
	pathWithRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.PathName)

	if err := os.MkdirAll(pathWithRoot, os.ModePerm); err != nil {
		return err
	}

	filepathWithRoot := fmt.Sprintf("%s/%s", s.Root, pathKey.FullPath())

	fmt.Println(filepathWithRoot)

	f, err := os.Create(filepathWithRoot)
	if err != nil {
		return err
	}

	defer f.Close()

	n, err := io.Copy(f, r)
	if err != nil {
		return err
	}

	fmt.Println("wrote", n, "bytes to", filepathWithRoot)

	return nil
}

func (s *Store) Clear() error {
	return os.RemoveAll(s.Root)
}
