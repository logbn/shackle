package service

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"

	"highvolume.io/shackle/api/intapi"
	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/log"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	waitPause           = 100 * time.Millisecond
)

type Coordination interface {
	Join(id, addr string) error
	GetClusterManifest() (entity.ClusterManifest, error)
	Start()
	Stop()
}

type coordination struct {
	intapi.UnimplementedCoordinationServer
	active       bool
	cfg          *config.Cluster
	intApiClient intapi.CoordinationClientFinder
	join         []config.NodeJoin
	log          log.Logger
	manifest     *entity.ClusterManifest
	mu           sync.Mutex
	initmutex    sync.Mutex
	nodeID       string
	obsChan      chan raft.Observation
	raft         *raft.Raft
	waiting      bool
	clock        clock.Clock
}

// NewCoordination returns a coordination service
func NewCoordination(
	cfg *config.App,
	log log.Logger,
	iac intapi.CoordinationClientFinder,
) (s *coordination, err error) {
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
		nodeID:       cfg.Cluster.Node.ID,
		log:          log,
		join:         join,
		manifest:     &entity.ClusterManifest{},
		cfg:          cfg.Cluster,
		intApiClient: iac,
		clock:        clock.New(),
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
	s.initmutex.Lock()
	defer s.initmutex.Unlock()
	s.clock.Sleep(waitPause)
	s.raft.Barrier(0).Error()
	s.waiting = false
	s.initialize()
}

// initialize is called by leaders and followers upon FSM synchronization.
func (s *coordination) initialize() {
	if s.active {
		return
	}
	state := s.raft.State()
	if state == raft.Leader {
		// Set data address if not set
		_, err := s.setNodeMeta(s.nodeID, s.cfg.Node.AddrIntApi, s.cfg.Node.Meta, s.cfg.Node.VNodeCount)
		if err != nil {
			s.log.Errorf(err.Error())
			return
		}
		// Stop if any nodes have not yet reported meta
		for _, node := range s.manifest.Catalog.Nodes {
			if node.AddrIntApi == "" {
				return
			}
		}
		// Schedule allocation
		if s.manifest.ClusterInitializing() {
			err := s.manifest.Allocate(s.cfg.Partitions)
			if err != nil {
				s.log.Errorf(err.Error())
				return
			}
			s.manifest.Status = entity.CLUSTER_STATUS_ALLOCATING
			data := s.manifest.ToJson()
			ftr := s.raft.Apply(data, raftTimeout)
			err = ftr.Error()
			if err != nil {
				err = fmt.Errorf("Error activating cluster: %s", err.Error())
				return
			}
			s.log.Debugf("%s Cluster Allocating.", s.nodeID)
			return
		}
		// Perform allocation
		node := s.manifest.GetNodeByID(s.nodeID)
		if s.manifest.ClusterAllocating() && node.Initializing() {
			err := s.allocate()
			if err != nil {
				err = fmt.Errorf("Error allocating leader: %s", err.Error())
				return
			} else {
				s.setNodeStatus(s.nodeID, entity.CLUSTER_NODE_STATUS_ALLOCATED)
			}
			return
		}
		// Stop if any nodes have not yet confirmed allocation
		for _, node := range s.manifest.Catalog.Nodes {
			if node.Initializing() {
				return
			}
		}
		// Activate Cluster
		if s.manifest.ClusterAllocating() {
			s.manifest.Status = entity.CLUSTER_STATUS_ACTIVE
			data := s.manifest.ToJson()
			ftr := s.raft.Apply(data, raftTimeout)
			err = ftr.Error()
			if err != nil {
				err = fmt.Errorf("Error activating cluster: %s", err.Error())
				return
			}
			s.log.Debugf("%s Cluster Activated. %s", s.nodeID, s.manifest.ToJson())
		}
		// Set status to active
		if s.manifest.ClusterActive() && node.Allocated() {
			_, err := s.setNodeStatus(s.nodeID, entity.CLUSTER_NODE_STATUS_ACTIVE)
			if err != nil {
				fmt.Errorf("Error updating node status to Active: %s", err.Error())
			}
			return
		}
		if node.Active() {
			s.log.Infof("Node %s Active.", s.nodeID)
			s.active = true
		}
	} else if state == raft.Follower {
		// Check manifest to see if leader's data address is represented.
		leaderAddrIntApi := s.getAddrIntApi(string(s.raft.Leader()))
		if len(leaderAddrIntApi) < 1 {
			s.log.Debugf("%s Leader data address not set", s.nodeID)
			return
		}
		// Check manifest to see if this node's data address is represented & correct.
		nodeAddrIntApi := s.getAddrIntApi(s.cfg.Node.AddrRaft)
		if nodeAddrIntApi != s.cfg.Node.AddrIntApi {
			s.updateNodeData(leaderAddrIntApi)
			s.log.Debugf("%s Updating data address", s.nodeID)
			return
		}
		// Perform allocation
		node := s.manifest.GetNodeByID(s.nodeID)
		if s.manifest.ClusterAllocating() && node.Initializing() {
			err := s.allocate()
			if err != nil {
				err = fmt.Errorf("Error allocating follower: %s", err.Error())
				return
			} else {
				err = s.updateNodeStatus(leaderAddrIntApi, entity.CLUSTER_NODE_STATUS_ALLOCATED)
				if err != nil {
					fmt.Errorf("Error updating node status to allocated: %s", err.Error())
				}
			}
			return
		}
		// Set status to active
		if s.manifest.ClusterActive() && node.Allocated() {
			err := s.updateNodeStatus(leaderAddrIntApi, entity.CLUSTER_NODE_STATUS_ACTIVE)
			if err != nil {
				fmt.Errorf("Error updating node status to Active: %s", err.Error())
			}
			return
		}
		if node.Active() {
			s.log.Infof("Node %s Active.", s.nodeID)
			s.active = true
		}
		if node.Down() {
			// Begin recovery
		}
	}
	return
}

// Apply applies a Raft log entry to the key-value store.
func (s *coordination) Apply(l *raft.Log) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
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

	return fsmSnapshot(s.manifest.ToJson()), nil
}

// Restore stores the key-value store to a previous state.
func (s *coordination) Restore(rc io.ReadCloser) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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

func (s *coordination) getAddrIntApi(addrRaft string) (addrIntApi string) {
	n := s.manifest.GetNodeByAddrRaft(addrRaft)
	if n != nil {
		addrIntApi = n.AddrIntApi
	}

	return
}

// Build initial manifest if none exists upon cluster bootstrap
func (s *coordination) initializeManifest() (err error) {
	if s.manifest.Catalog.Version != "" {
		return
	}
	var conf = s.raft.GetConfiguration().Configuration()
	var nodes = make([]entity.ClusterNode, len(conf.Servers))
	for i, srv := range conf.Servers {
		nodes[i] = entity.ClusterNode{
			ID:       string(srv.ID),
			AddrRaft: string(srv.Address),
			Status:   entity.CLUSTER_NODE_STATUS_INITIALIZING,
		}
	}
	s.manifest = &entity.ClusterManifest{
		ID:     s.cfg.ID,
		Status: entity.CLUSTER_STATUS_INITIALIZING,
		Catalog: entity.ClusterCatalog{
			Version:    "1.0.0",
			Replicas:   s.cfg.Replicas,
			Surrogates: s.cfg.Surrogates,
			Nodes:      nodes,
		},
	}
	ftr := s.raft.Apply(s.manifest.ToJson(), raftTimeout)
	err = ftr.Error()
	if err != nil {
		s.log.Errorf("Error initializing manifest: %s", err.Error())
		return
	}
	s.log.Debugf("%s Manifest Initialized %s", s.nodeID, string(s.manifest.ToJson()))

	return nil
}

// Initialize vnodes. Special operation of the persistence service.
func (s *coordination) allocate() (err error) {
	s.log.Debugf("%s Allocating", s.nodeID)
	return
}

// Start starts the service
func (s *coordination) Start() {
	go func() {
		for {
			select {
			case leader := <-s.raft.LeaderCh():
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
					s.initializeManifest()
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

// Makes GRPC call to leader to update node meta
func (s *coordination) updateNodeData(leaderAddrIntApi string) (err error) {
	c, err := s.intApiClient.Get(leaderAddrIntApi)
	if err != nil {
		return
	}
	_, err = c.NodeUpdate(context.Background(), &intapi.NodeUpdateRequest{
		Id:         s.nodeID,
		AddrIntApi: s.cfg.Node.AddrIntApi,
		Meta:       s.cfg.Node.Meta,
		VNodeCount: uint32(s.cfg.Node.VNodeCount),
	})
	return
}

// Receives GRPC call for node meta update
func (s *coordination) NodeUpdate(ctx context.Context, req *intapi.NodeUpdateRequest) (*intapi.NodeUpdateReply, error) {
	success, err := s.setNodeMeta(req.Id, req.AddrIntApi, req.Meta, int(req.VNodeCount))
	return &intapi.NodeUpdateReply{Success: success}, err
}

// Sets node meta on leader applying manifest changes if altered
func (s *coordination) setNodeMeta(nodeID string, addrIntApi string, meta map[string]string, vnodecount int,
) (updated bool, err error) {
	n := s.manifest.GetNodeByID(nodeID)
	if n == nil {
		err = fmt.Errorf("Node not found in setNodeMeta %s", nodeID)
		return
	}
	var dirty bool
	if n.AddrIntApi != addrIntApi {
		n.AddrIntApi = addrIntApi
		dirty = true
	}
	if n.VNodeCount != vnodecount {
		n.VNodeCount = vnodecount
		dirty = true
	}
	if !reflect.DeepEqual(n.Meta, meta) {
		n.Meta = meta
		dirty = true
	}
	if !dirty {
		return
	}
	ftr := s.raft.Apply(s.manifest.ToJson(), raftTimeout)
	err = ftr.Error()
	if err != nil {
		err = fmt.Errorf("Error updating internal api address: %s", err.Error())
		return
	}
	s.log.Debugf("%s Internal API address set. %s = %s", s.nodeID, nodeID, addrIntApi)

	return true, nil
}

// Makes GRPC call to leader to update node meta
func (s *coordination) updateNodeStatus(leaderAddrIntApi, status string) (err error) {
	c, err := s.intApiClient.Get(leaderAddrIntApi)
	if err != nil {
		return
	}
	_, err = c.NodeStatusUpdate(context.Background(), &intapi.NodeStatusUpdateRequest{
		Id:     s.nodeID,
		Status: status,
	})
	return
}

// Receives GRPC call for node status update
func (s *coordination) NodeStatusUpdate(ctx context.Context, req *intapi.NodeStatusUpdateRequest) (*intapi.NodeUpdateReply, error) {
	success, err := s.setNodeStatus(req.Id, req.Status)
	return &intapi.NodeUpdateReply{Success: success}, err
}

// Sets node status on leader applying manifest changes if altered
func (s *coordination) setNodeStatus(nodeID, status string) (updated bool, err error) {
	n := s.manifest.GetNodeByID(nodeID)
	if n == nil {
		err = fmt.Errorf("Node not found in setNodeStatus %s", nodeID)
		return
	}
	if n.Status == status {
		return
	}
	n.Status = status
	ftr := s.raft.Apply(s.manifest.ToJson(), raftTimeout)
	err = ftr.Error()
	if err != nil {
		err = fmt.Errorf("Error updating node status: %s %s %s", nodeID, status, err.Error())
		return
	}

	return true, nil
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
