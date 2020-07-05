package grpcint

import (
	"sync"

	"google.golang.org/grpc"
)

type DelegationClientFinder interface {
	Get(addr string) (DelegationClient, error)
	Close()
}

type delegationClientFinder struct {
	clients map[string]DelegationClient
	conns   map[string]*grpc.ClientConn
	mutex   sync.RWMutex
}

func NewDelegationClientFinder() *delegationClientFinder {
	return &delegationClientFinder{
		map[string]DelegationClient{},
		map[string]*grpc.ClientConn{},
		sync.RWMutex{},
	}
}

func (f *delegationClientFinder) Get(addr string) (c DelegationClient, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if c, ok := f.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return
	}
	c = NewDelegationClient(conn)
	if err == nil {
		f.clients[addr] = c
		f.conns[addr] = conn
	}
	return
}

func (f *delegationClientFinder) Close() {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	for _, c := range f.conns {
		c.Close()
	}
	f.clients = map[string]DelegationClient{}
	f.conns = map[string]*grpc.ClientConn{}
}
