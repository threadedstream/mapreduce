package coordinator

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"

	"go.uber.org/zap"

	coordinatorapi "github.com/threadedstream/mapreduce/api/coordinator"
	workerapi "github.com/threadedstream/mapreduce/api/worker"
	"golang.org/x/sync/errgroup"
)

// Coordinator is central element, that serves user requests and coordinates worker nodes

// Main is an entrypoint for coordinator
func Main(ctx context.Context, logger *zap.Logger) error {
	_, err := NewCoordinator(ctx, logger)
	return err
}

type Coordinator struct {
	baseCtx     context.Context
	server      *rpc.Server
	workerNodes map[string]struct{}
	mu          sync.Mutex // protects access to workerNodes
	logger      *zap.Logger
}

func NewCoordinator(ctx context.Context, logger *zap.Logger) (*Coordinator, error) {
	c := &Coordinator{
		server:      rpc.NewServer(),
		baseCtx:     ctx,
		workerNodes: make(map[string]struct{}),
		logger:      logger,
	}

	if err := c.server.Register(c); err != nil {
		return nil, err
	}

	if err := c.startListening(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Coordinator) startListening() error {
	lis, err := net.Listen("tcp4", ":8000")
	if err != nil {
		return err
	}
	go func() {
		c.server.Accept(lis)
	}()

	return nil
}

func (c *Coordinator) RegisterMe(req *coordinatorapi.RegisterMeRequest) (*coordinatorapi.RegisterMeReply, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger.Info("adding new node", zap.String("node", req.MyAddress))
	c.workerNodes[req.MyAddress] = struct{}{}
	return &coordinatorapi.RegisterMeReply{}, nil
}

func (c *Coordinator) Process(req *coordinatorapi.ProcessRequest) (*coordinatorapi.ProcessReply, error) {
	// split up corpus and send it to worker nodes
	id := randid()
	startIdx, endIdx := 0, 0
	chunkSize := len(req.Corpus) / len(c.workerNodes)

	eg, _ := errgroup.WithContext(context.Background())
	eg.SetLimit(len(c.workerNodes))

	for workerNode := range c.workerNodes {
		startIdx = endIdx
		endIdx = startIdx + chunkSize
		chunk := req.Corpus[startIdx:endIdx]
		eg.Go(func() error {
			return c.mapChunk(id, workerNode, chunk)
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return &coordinatorapi.ProcessReply{}, nil
}

func (c *Coordinator) mapChunk(id string, workerNode string, chunk []byte) error {
	cli, err := rpc.Dial("tcp4", workerNode)
	if err != nil {
		return err
	}
	req := &workerapi.MapRequest{
		ID:    id,
		Chunk: chunk,
	}

	reply := &workerapi.MapReply{}
	if err = cli.Call("Worker.Map", req, reply); err != nil {
		return err
	}

	return nil
}

func (c *Coordinator) heartbeatEvery(ctx context.Context, tick time.Duration) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	select {
	case <-ctx.Done():
		return
	case <-ticker.C:
		wg := sync.WaitGroup{}
		wg.Add(len(c.workerNodes))
		for workerNode := range c.workerNodes {
			go func() {
				if err := c.heartbeat(workerNode); err != nil {
					c.mu.Lock()
					c.logger.Error(fmt.Sprintf("failed to reach node %s", workerNode))
					delete(c.workerNodes, workerNode)
					c.mu.Unlock()
				}
			}()
		}
		wg.Wait()
	}
}

func (c *Coordinator) heartbeat(node string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	return sendAsync(ctx, node, "Worker.Heartbeat", &workerapi.HeartBeatRequest{}, &workerapi.HeartbeatReply{})
}

const alphabet = "0123456789abcdefghijklmnoprstuvwxyz$&"

func randid() string {
	const size = len(alphabet)
	res := make([]byte, 8)
	for i := range 8 {
		res[i] = alphabet[rand.Intn(size)]
	}
	return string(res)
}

func sendAsync(ctx context.Context, addr, method string, args, reply any) error {
	cli, err := rpc.Dial("tcp4", addr)
	if err != nil {
		return errors.New("node unreachable")
	}

	done := make(chan *rpc.Call)
	_ = cli.Go(method, args, reply, done)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case call := <-done:
		if call.Error != nil {
			return call.Error
		}
	}
	return nil
}
