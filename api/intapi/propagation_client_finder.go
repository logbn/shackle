package intapi

import (
	"sync"

	"google.golang.org/grpc"
)

type PropagationClientFinder interface {
	Get(addr string) (PropagationClient, error)
	Close()
}

type propagationClientFinder struct {
	clients map[string]PropagationClient
	conns   map[string]*grpc.ClientConn
	mutex   sync.RWMutex
}

func NewPropagationClientFinder() *propagationClientFinder {
	return &propagationClientFinder{
		map[string]PropagationClient{},
		map[string]*grpc.ClientConn{},
		sync.RWMutex{},
	}
}

func (f *propagationClientFinder) Get(addr string) (c PropagationClient, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if c, ok := f.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return
	}
	c = NewPropagationClient(conn)
	if err == nil {
		f.clients[addr] = c
		f.conns[addr] = conn
	}
	return
}

func (f *propagationClientFinder) Close() {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	for _, c := range f.conns {
		c.Close()
	}
	f.clients = map[string]PropagationClient{}
	f.conns = map[string]*grpc.ClientConn{}
}
