package p2p


type  peer interface{
	Close() error
}

type transport interface {
   ListenAndAccept()error
   Consume()<-chan Message
}
