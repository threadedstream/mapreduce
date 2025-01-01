package coordinator

type ProcessRequest struct {
	Corpus []byte
}

type ProcessReply struct {
}

type RegisterMeRequest struct {
	MyAddress string
}

type RegisterMeReply struct{}
