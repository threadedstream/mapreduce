package worker

type MapRequest struct {
	ID    string
	Chunk []byte
}

type MapReply struct {
	ID string
}

type HeartBeatRequest struct {
}

type HeartbeatReply struct {
}
