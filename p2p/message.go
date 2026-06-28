package p2p


const(
	IncomingMessage = 0x1
	IncomingStream= 0x2
)

type Message struct {
	From    string
	Payload  []byte
	Stream bool
}
