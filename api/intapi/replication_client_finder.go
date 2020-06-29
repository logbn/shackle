package intapi

import (
	"sync"

	"google.golang.org/grpc"
)

type ReplicationClientFinder interface {
	Get(addr string) (ReplicationClient, error)
	Close()
}

type replicationClientFinder struct {
	clients map[string]ReplicationClient
	conns   map[string]*grpc.ClientConn
	mutex   sync.RWMutex
}

func NewReplicationClientFinder() *replicationClientFinder {
	return &replicationClientFinder{
		map[string]ReplicationClient{},
		map[string]*grpc.ClientConn{},
		sync.RWMutex{},
	}
}

func (f *replicationClientFinder) Get(addr string) (c ReplicationClient, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if c, ok := f.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return
	}
	c = NewReplicationClient(conn)
	if err == nil {
		f.clients[addr] = c
		f.conns[addr] = conn
	}
	return
}

func (f *replicationClientFinder) Close() {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	for _, c := range f.conns {
		c.Close()
	}
	f.clients = map[string]ReplicationClient{}
	f.conns = map[string]*grpc.ClientConn{}
}
