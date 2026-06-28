package p2p

import (
	"encoding/gob"
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
 peekBuf:=make([]byte,1)
 if _,err:=r.Read(peekBuf);err!=nil{
	 return nil
 }

stream:=peekBuf[0]==IncomingStream
if stream{
	msg.Stream=true
	return nil
}

	buf := make([]byte, 1028)
	n, err := r.Read(buf)
	if err != nil {
		return err
	}
	msg.Payload = buf[:n]
	return nil
}
