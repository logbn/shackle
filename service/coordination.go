package service

import (
	"context"
	"fmt"
	"math"
	"time"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/lni/dragonboat/v3"
	dbconf "github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/raftio"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
	dblog "github.com/lni/dragonboat/v3/logger"

	"highvolume.io/shackle/api/grpcint"
	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/log"
)

const (
	raftTimeout         = 10 * time.Second
	metaClusterID = math.MaxUint64
)

type Coordination interface {
	// Join(id, addr string) error
	PlanDelegation(entity.Batch) (entity.BatchPlan, error)
	Start() error
	Stop()
}

type coordination struct {
	grpcint.UnimplementedCoordinationServer
	grpcIntClient   grpcint.CoordinationClientFinder
	join            []config.HostJoin
	log             log.Logger
	manifest        *entity.Manifest
	mu              sync.RWMutex
	initmutex       sync.Mutex
	hostID          uint64
	raftAddr        string
	raftSolo        bool
	initChan        chan entity.Catalog
	nodeHost        *dragonboat.NodeHost
	active          bool
	waiting         bool
	allocated       bool
	clock           clock.Clock
	metaNodeFactory func(uint64, uint64) dbsm.IStateMachine
	cfg             *config.App
}

// NewCoordination returns a coordination service
func NewCoordination(
	cfg *config.App,
	log log.Logger,
	iac grpcint.CoordinationClientFinder,
	metaNodeFactory func(uint64, uint64) dbsm.IStateMachine,
) (s *coordination, initChan chan entity.Catalog, err error) {
	var (
		hostID       = cfg.Host.ID
		deploymentID = cfg.Host.DeploymentID
		raftAddr     = cfg.Host.RaftAddr
		raftDir      = cfg.Host.RaftDir
		raftSolo     = cfg.Host.RaftSolo
		join         = cfg.Host.Join
	)
	if len(raftAddr) < 1 {
		err = fmt.Errorf("Node Address required")
		return
	}
	if hostID < 1 {
		err = fmt.Errorf("Node Host ID required (cannot be 0)")
		return
	}
	if deploymentID < 1 {
		err = fmt.Errorf("Deployment ID required (cannot be 0)")
		return
	}
	dblog.GetLogger("raft").SetLevel(dblog.INFO)
	dblog.GetLogger("rsm").SetLevel(dblog.INFO)
	dblog.GetLogger("transport").SetLevel(dblog.INFO)
	dblog.GetLogger("grpc").SetLevel(dblog.INFO)
	s = &coordination{
		hostID:          hostID,
		log:             log,
		join:            join,
		manifest:        &entity.Manifest{},
		grpcIntClient:   iac,
		clock:           clock.New(),
		initChan:        make(chan entity.Catalog),
		cfg:             cfg,
		raftAddr:        raftAddr,
		metaNodeFactory: metaNodeFactory,
		raftSolo:        raftSolo,
	}
	s.nodeHost, err = dragonboat.NewNodeHost(dbconf.NodeHostConfig{
		DeploymentID:      deploymentID,
		NodeHostDir:       raftDir,
		RTTMillisecond:    200,
		RaftAddress:       raftAddr,
		EnableMetrics:     true,
		RaftEventListener: s,
	})
	if err != nil {
		return
	}
	return
}

// PlanDelegation returns a batch delegation plan
// Batches are delegated to partition master nodes
func (s *coordination) PlanDelegation(batch entity.Batch) (plan entity.BatchPlan, err error) {
	plan = entity.BatchPlan{}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var hostMap = s.manifest.Catalog.GetPartitionMap()
	for clusterID, b := range batch.Partitioned() {
		// TODO - rotate host based on retry number
		// TODO - rotate host based on circuit breaker failure
		var hostID = hostMap[clusterID][0]
		if hostID != s.hostID && containsUint64(hostMap[clusterID], s.hostID) {
			hostID = s.hostID
		}
		if _, ok := plan[hostID]; !ok {
			host := s.manifest.GetHostByID(hostID)
			plan[hostID] = &entity.BatchPlanSegment{
				NodeAddr: host.IntApiAddr,
				Batch:    entity.Batch{},
			}
		}
		plan[hostID].Batch = append(plan[hostID].Batch, b...)
	}
	return
}

func (s *coordination) getAddrIntApi(raftAddr string) (intApiAddr string) {
	h := s.manifest.GetHostByRaftAddr(raftAddr)
	if h != nil {
		intApiAddr = h.IntApiAddr
	}

	return
}

// Build initial manifest if none exists upon cluster bootstrap
func (s *coordination) initializeManifest() (err error) {
	if s.manifest.Catalog.Version != "" {
		return
	}
	var hosts = make([]entity.Host, len(s.cfg.Host.Join) + 1)
	hosts[0] = entity.Host{
		ID:       s.cfg.Host.ID,
		RaftAddr: s.cfg.Host.RaftAddr,
		Status:   entity.HOST_STATUS_INITIALIZING,
	}
	for i, h := range s.cfg.Host.Join {
		hosts[i+1] = entity.Host{
			ID:       h.ID,
			RaftAddr: h.RaftAddr,
			Status:   entity.HOST_STATUS_INITIALIZING,
		}
	}
	s.manifest = &entity.Manifest{
		Version: 1,
		DeploymentID: s.cfg.Host.DeploymentID,
		Status: entity.DEPLOYMENT_STATUS_INITIALIZING,
		Catalog: entity.Catalog{
			Version:      "1.0.0",
			ReplicaCount: s.cfg.Host.ReplicaCount,
			WitnessCount: s.cfg.Host.WitnessCount,
			Hosts:        hosts,
		},
	}
	ctx, _ := context.WithTimeout(context.Background(), raftTimeout)
	metaSession, err := s.nodeHost.SyncGetSession(ctx, metaClusterID)
	if err != nil {
		s.log.Errorf("Error getting meta session: %s", err.Error())
		return
	}
	ctx, _ = context.WithTimeout(context.Background(), raftTimeout)
	_, err = s.nodeHost.SyncPropose(ctx, metaSession, s.manifest.ToJson())
	if err != nil {
		s.log.Errorf("Error proposing initial manifest: %s", err.Error())
		return
	}
	s.log.Debugf("%s Manifest Initialized", s.hostID)

	return nil
}

// Initialize vnodes. Special operation of the persistence service.
func (s *coordination) allocate() (err error) {
	s.initChan <- s.manifest.Catalog
	return
}

// Start starts the service
func (s *coordination) Start() (err error) {
	var initialMembers = map[uint64]string{
		s.hostID: s.raftAddr,
	}
	if len(s.join) > 0 {
		for _, host := range s.join {
			initialMembers[host.ID] = host.RaftAddr
		}
	}
	clusterConfig := dbconf.Config{
		NodeID:             s.hostID,
		ClusterID:          metaClusterID,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
	}
	err = s.nodeHost.StartCluster(initialMembers, !s.raftSolo, s.metaNodeFactory, clusterConfig)
	if err != nil {
		return
	}
	return
}

func (s *coordination) LeaderUpdated(info raftio.LeaderInfo) {
	if s.manifest.Version == 0 && info.LeaderID == info.NodeID {
		s.initializeManifest()
	}
}

func (s *coordination) Stop() {
	close(s.initChan)
	s.grpcIntClient.Close()
}

func containsUint64(slice []uint64, contains uint64) bool {
	for _, value := range slice {
		if value == contains {
			return true
		}
	}
	return false
}
