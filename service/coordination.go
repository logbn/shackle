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
	GetClusterManifest() (entity.ClusterManifest, error)
	Start()
	Stop()
}

type coordination struct {
	nodeID   string
	raft     *raft.Raft
	log      log.Logger
	mu       sync.Mutex
	manifest *entity.ClusterManifest
	join     []config.NodeJoin
	obsChan  chan raft.Observation
	cfg      *config.Cluster
	active   bool
	waiting  bool
}

// NewCoordination returns a coordination service
func NewCoordination(cfg *config.App, log log.Logger) (s *coordination, err error) {
	var (
		nodeID   = cfg.Cluster.Node.ID
		addrRaft = cfg.Cluster.Node.AddrRaft
		raftDir  = cfg.Cluster.Node.RaftDir
		raftSolo = cfg.Cluster.Node.RaftSolo
		join     = cfg.Cluster.Node.Join
	)
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	addr, err := net.ResolveTCPAddr("tcp", addrRaft)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(addrRaft, addr, 3, 10*time.Second, os.Stderr)
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
	s = &coordination{
		nodeID:   cfg.Cluster.Node.ID,
		log:      log,
		join:     join,
		manifest: &entity.ClusterManifest{},
		cfg:      cfg.Cluster,
	}
	s.raft, err = raft.NewRaft(config, s, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %s", err)
	}
	if raftSolo {
		s.raft.BootstrapCluster(raft.Configuration{
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
				s.log.Infof("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
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

func (s *coordination) GetClusterManifest() (status entity.ClusterManifest, err error) {
	return *s.manifest, nil
}

// wait uses barrier to call initialize after FSM sync
func (s *coordination) wait() {
	s.raft.Barrier(0).Error()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.waiting = false
	s.initialize()
}

// initialize is called by leaders and followers upon FSM synchronization.
func (s *coordination) initialize() {
	if s.active {
		return
	}
	if s.raft.State() == raft.Leader {
		s.log.Debugf("%s Leader init %s", s.nodeID, s.manifest.ToJson())
		// Set data address if not set
		updated, err := s.setDataAddr(s.nodeID, s.cfg.Node.AddrData)
		if updated {
			return
		}
		if err != nil {
			s.log.Errorf(err.Error())
			return
		}
	} else if s.raft.State() == raft.Follower {
		s.log.Debugf("%s Follower init", s.nodeID)
		// Check manifest to see if leader's data address is represented.
		leaderAddrData := s.getLeaderAddrData()
		if len(leaderAddrData) < 1 {
			return
		}
		// Check manifest to see if this node's data address is represented & correct.
		// If not, send status to leader and return.
		// Check manifest to ensure data address present for all nodes.
		// If not, return.
		// Check manifest to ensure cluster is active
		// If not, request cluster activation from leader.
		// Set cluster to active and return.
		s.active = true
		return
	}
}

// Apply applies a Raft log entry to the key-value store.
func (s *coordination) Apply(l *raft.Log) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log.Debugf("%s FSM Apply", s.nodeID)
	if !s.active && !s.waiting {
		s.waiting = true
		go s.wait()
	}
	err := s.manifest.FromJson(l.Data)
	if err != nil {
		s.log.Errorf("Error parsing raft log: %s", err.Error())
		return err
	}

	return nil
}

// Snapshot returns a snapshot of the key-value store.
func (s *coordination) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log.Debugf("%s FSM Snapshot", s.nodeID)

	return fsmSnapshot(s.manifest.ToJson()), nil
}

// Restore stores the key-value store to a previous state.
func (s *coordination) Restore(rc io.ReadCloser) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log.Debugf("%s FSM Restore", s.nodeID)
	if !s.active && !s.waiting {
		s.waiting = true
		go s.wait()
	}
	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return err
	}
	s.manifest.FromJson(data)

	return nil
}

func (s *coordination) getLeaderAddrData() (addrData string) {
	leader := s.raft.Leader()
	n := s.manifest.GetNodeByAddrRaft(string(leader))
	if n != nil {
		addrData = n.AddrData
	}

	return
}

func (s *coordination) setDataAddr(nodeID, addrData string) (updated bool, err error) {
	n := s.manifest.GetNodeByID(nodeID)
	if n == nil {
		err = fmt.Errorf("Node not found in setDataAddr %s", nodeID)
		return
	}
	if n.AddrData == addrData {
		return
	}
	n.AddrData = addrData
	ftr := s.raft.Apply(s.manifest.ToJson(), raftTimeout)
	err = ftr.Error()
	if err != nil {
		err = fmt.Errorf("Error updating data address: %s", err.Error())
		return
	}
	s.log.Debugf("%s Data address set %s", nodeID, addrData)

	return true, nil
}

func (s *coordination) initializeManifest() (err error) {
	var conf = s.raft.GetConfiguration().Configuration()
	var nodes = make([]entity.ClusterNode, len(conf.Servers))
	for i, srv := range conf.Servers {
		nodes[i] = entity.ClusterNode{
			ID:       string(srv.ID),
			AddrRaft: string(srv.Address),
		}
	}
	s.manifest = &entity.ClusterManifest{
		ID: s.cfg.ID,
		Catalog: entity.ClusterCatalog{
			Version:    "1.0.0",
			Replicas:   s.cfg.Replicas,
			Surrogates: s.cfg.Surrogates,
			Nodes:      nodes,
		},
	}
	ftr := s.raft.Apply(s.manifest.ToJson(), time.Minute)
	err = ftr.Error()
	if err != nil {
		s.log.Errorf("Error initializing manifest: %s", err.Error())
		return
	}
	s.log.Debugf("%s Manifest Initialized", s.nodeID)

	return nil
}

func (s *coordination) Start() {
	go func() {
		for {
			select {
			case leader := <-s.raft.LeaderCh():
				s.mu.Lock()
				defer s.mu.Unlock()
				if leader {
					s.log.Debugf("%s Became Leader", s.nodeID)
					for _, j := range s.join {
						if len(j.ID) > 0 && len(j.AddrRaft) > 0 {
							err := s.Join(j.ID, j.AddrRaft)
							if err != nil {
								s.log.Errorf(err.Error())
							}
						}
					}
					// Wait for FSM log replay
					s.raft.Barrier(0).Error()
					s.startObserver()
					if s.manifest.Catalog.Version == "" {
						s.initializeManifest()
					}
				} else {
					s.log.Debugf("%s Became Follower", s.nodeID)
				}
			}
		}
	}()
}

func (s *coordination) startObserver() {
	s.obsChan = make(chan raft.Observation)
	s.raft.RegisterObserver(raft.NewObserver(s.obsChan, true, nil))
	go func() {
		for {
			select {
			case o := <-s.obsChan:
				// Main event loop for responding to changes in cluster state
				s.log.Debugf("OBS - %#v", o.Data)
			}
		}
	}()
}

func (s *coordination) Stop() {
}

type fsmSnapshot []byte

func (f fsmSnapshot) Persist(sink raft.SnapshotSink) (err error) {
	_, err = sink.Write(f)
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

func (f fsmSnapshot) Release() {}
