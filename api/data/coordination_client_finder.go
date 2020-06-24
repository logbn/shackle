package data

import (
	"sync"

	"google.golang.org/grpc"
)

type CoordinationClientFinder interface {
	Get(addr string) (CoordinationClient, error)
	Close()
}

type coordinationClientFinder struct {
	clients map[string]CoordinationClient
	conns   map[string]*grpc.ClientConn
	mutex   sync.RWMutex
}

func NewCoordinationClientFinder() *coordinationClientFinder {
	return &coordinationClientFinder{
		map[string]CoordinationClient{},
		map[string]*grpc.ClientConn{},
		sync.RWMutex{},
	}
}

func (f *coordinationClientFinder) Get(addr string) (c CoordinationClient, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if c, ok := f.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return
	}
	c = NewCoordinationClient(conn)
	if err == nil {
		f.clients[addr] = c
		f.conns[addr] = conn
	}
	return
}

func (f *coordinationClientFinder) Close() {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	for _, c := range f.conns {
		c.Close()
	}
	f.clients = map[string]CoordinationClient{}
	f.conns = map[string]*grpc.ClientConn{}
}
