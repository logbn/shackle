package service

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"

	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/log"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type Coordination interface {
	Join(id, addr string) error
	GetClusterManifest() (status *entity.ClusterManifest, err error)
	Start()
	Stop()
}

type coordination struct {
	raft     *raft.Raft
	log      log.Logger
	mu       sync.Mutex
	manifest *entity.ClusterManifest
	joins    []config.NodeJoin
	obsChan  chan raft.Observation
}

// NewCoordination returns a coordination service
func NewCoordination(cfg *config.App, log log.Logger) (r *coordination, err error) {
	var (
		nodeID   = cfg.Cluster.Node.ID
		raftPort = cfg.Cluster.Node.RaftPort
		raftDir  = cfg.Cluster.Node.RaftDir
		raftSolo = cfg.Cluster.Node.RaftSolo
		join     = cfg.Cluster.Node.Join
	)
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", raftPort))
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(fmt.Sprintf(":%d", raftPort), addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}
	snapshots, err := raft.NewFileSnapshotStore(raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("file snapshot store: %s", err)
	}
	var logStore raft.LogStore
	var stableStore raft.StableStore
	if len(raftDir) == 0 {
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
	} else {
		boltDB, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
		if err != nil {
			return nil, fmt.Errorf("new bolt store: %s", err)
		}
		logStore = boltDB
		stableStore = boltDB
	}
	r = &coordination{log: log, joins: join, manifest: &entity.ClusterManifest{}}
	r.raft, err = raft.NewRaft(config, r, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %s", err)
	}
	if raftSolo {
		r.raft.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			}},
		})
	}

	return
}

// Join joins a node, identified by nodeID and located at addr, to this store.
func (s *coordination) Join(nodeID, addr string) error {
	s.log.Debugf("received join request for remote node %s at %s", nodeID, addr)
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.log.Errorf("failed to get raft configuration: %v", err)
		return err
	}
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				s.log.Errorf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	s.log.Debugf("node %s at %s joined successfully", nodeID, addr)
	return nil
}

func (s *coordination) GetClusterManifest() (status *entity.ClusterManifest, err error) {
	return s.manifest, nil
}

// Apply applies a Raft log entry to the key-value store.
func (f *coordination) Apply(l *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.manifest.FromJson(l.Data)
	if err != nil {
		f.log.Errorf("Error parsing raft log: %s", err.Error())
		return err
	}
	return nil
}

// Snapshot returns a snapshot of the key-value store.
func (f *coordination) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return &fsmSnapshot{manifest: f.manifest.ToJson()}, nil
}

// Restore stores the key-value store to a previous state.
func (f *coordination) Restore(rc io.ReadCloser) error {
	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return err
	}
	f.manifest.FromJson(data)
	return nil
}

func (s *coordination) Start() {
	s.obsChan = make(chan raft.Observation)
	s.raft.RegisterObserver(raft.NewObserver(s.obsChan, true, nil))
	go func() {
		for {
			select {
			case o := <-s.obsChan:
				// Main event loop for responding to changes in cluster state
				s.log.Debugf("%#v", o.Data)
			}
		}
	}()
	go func() {
		select {
		case <-s.raft.LeaderCh():
			for _, j := range s.joins {
				if len(j.ID) > 0 && len(j.Addr) > 0 {
					err := s.Join(j.ID, j.Addr)
					if err != nil {
						s.log.Errorf(err.Error())
					}
				}
			}
		}
	}()
}

func (s *coordination) Stop() {

}

type fsmSnapshot struct {
	manifest []byte
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) (err error) {
	_, err = sink.Write(f.manifest)
	if err != nil {
		sink.Cancel()
		return
	}
	err = sink.Close()
	if err != nil {
		sink.Cancel()
		return
	}
	return
}

func (f *fsmSnapshot) Release() {}
