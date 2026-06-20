package p2p

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
)

type Decoder interface {
	Decode(io.Reader, *Message) error
}

type GobDecoder struct{}

func (g GobDecoder) Decode(r io.Reader, msg *Message) error {
	return gob.NewDecoder(r).Decode(&msg.Payload)
}

type DefaultDecoder struct{}

func (d DefaultDecoder) Decode(r io.Reader, msg *Message) error {

	reader := bufio.NewReader(r)

	line, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("disconnected:", err)
		return err
	}

	msg.Payload = []byte(line)

	return nil

}
