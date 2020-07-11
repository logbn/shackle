package cluster

import (
	"fmt"
	"io"
	"reflect"
	"sync"
	"context"
	"math"
	"time"
	"io/ioutil"

	"github.com/benbjohnson/clock"
	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/v3"
	dbconf "github.com/lni/dragonboat/v3/config"
	dblog "github.com/lni/dragonboat/v3/logger"
	dbio "github.com/lni/dragonboat/v3/raftio"
	dbsm "github.com/lni/dragonboat/v3/statemachine"

	"highvolume.io/shackle/api/grpcint"
	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/entity/event"
	"highvolume.io/shackle/log"
	"highvolume.io/shackle/service"
)

const (
	EVENT_SCHEMA_VERSION uint8 = 0

	EVENT_TYPE_NULL               uint8 = 0
	EVENT_TYPE_INIT               uint8 = 1
	EVENT_TYPE_TICK               uint8 = 2
	EVENT_TYPE_HOST_META_UPDATE   uint8 = 3
	EVENT_TYPE_HOST_STATUS_UPDATE uint8 = 4

	EVENT_RESPONSE_FAILURE uint64 = 0
	EVENT_RESPONSE_SUCCESS uint64 = 1

	metaClusterID = math.MaxUint64
	raftTimeout = 3 * time.Second
)

// Host runs the deployment's meta cluster.
// It boostraps the deployment and manages cluster configuration by running as a node in its own raft cluster
type Host interface {
	// statemachine.IStateMachine
	Update([]byte) (dbsm.Result, error)
	Lookup(interface{}) (interface{}, error)
	SaveSnapshot(io.Writer, dbsm.ISnapshotFileCollection, <-chan struct{}) error
	RecoverFromSnapshot(io.Reader, []dbsm.SnapshotFile, <-chan struct{}) error
	Close() error

	Lock(entity.Batch) ([]uint8, error)
	Commit(entity.Batch) ([]uint8, error)
	Rollback(entity.Batch) ([]uint8, error)

	GetInitChan() chan entity.Catalog
	PlanDelegation(batch entity.Batch) (plan entity.BatchPlan, err error)
	Start() error
	Stop()
}

type host struct {
	mutex          sync.RWMutex
	initmutex      sync.Mutex
	log            log.Logger
	cfg            config.Host
	id             uint64
	clusterID      uint64
	nodeID         uint64
	leaderID       uint64
	clock          clock.Clock
	nodeHost       *dragonboat.NodeHost
	manifest       *entity.Manifest
	svcHash        service.Hash
	svcPersistence service.Persistence
	svcDelegation  service.Delegation
	initChan       chan entity.Catalog
	active         bool
	starting       bool
	keylen         int
	factory        func(uint64, uint64) dbsm.IStateMachine
	nodes          map[uint64]Node
}

// NewHost returns a new host
func NewHost(
	cfg config.Host,
	log log.Logger,
	svcHash service.Hash,
	svcPersistence service.Persistence,
	svcDelegation service.Delegation,
	initChan chan entity.Catalog,
) (h *host, err error) {
	if len(cfg.RaftAddr) < 1 {
		err = fmt.Errorf("Host Address required")
		return
	}
	if cfg.ID < 1 {
		err = fmt.Errorf("Host ID required (cannot be 0)")
		return
	}
	if cfg.DeploymentID < 1 {
		err = fmt.Errorf("Deployment ID required (cannot be 0)")
		return
	}
	h = &host{
		log:            log,
		cfg:            cfg,
		id:             cfg.ID,
		manifest:       &entity.Manifest{},
		svcHash:        svcHash,
		svcPersistence: svcPersistence,
		svcDelegation:  svcDelegation,
		initChan:       make(chan entity.Catalog),
		keylen:         cfg.KeyLength,
		clock:          clock.NewMock(),
		nodes:          make(map[uint64]Node),
	}
	h.factory = func(clusterID uint64, nodeID uint64) dbsm.IStateMachine {
		h.clusterID = clusterID
		h.nodeID = nodeID
		return h
	}
	dblog.GetLogger("raft").SetLevel(dblog.INFO)
	dblog.GetLogger("rsm").SetLevel(dblog.INFO)
	dblog.GetLogger("transport").SetLevel(dblog.INFO)
	dblog.GetLogger("grpc").SetLevel(dblog.INFO)
	h.nodeHost, err = dragonboat.NewNodeHost(dbconf.NodeHostConfig{
		DeploymentID:      cfg.DeploymentID,
		NodeHostDir:       cfg.RaftDir,
		RTTMillisecond:    1000,
		RaftAddress:       cfg.RaftAddr,
		EnableMetrics:     true,
		RaftEventListener: h,
	})
	return
}

// Lock locks a batch for processing
func (h *host) Lock(batch entity.Batch) (res []uint8, err error) {
	return h.handleBatch(entity.OP_LOCK, batch)
}

// Commit commits a previously locked batch
func (h *host) Commit(batch entity.Batch) (res []uint8, err error) {
	return h.handleBatch(entity.OP_COMMIT, batch)
}

// Rollback rolls back a previously locked batch
func (h *host) Rollback(batch entity.Batch) (res []uint8, err error) {
	return h.handleBatch(entity.OP_ROLLBACK, batch)
}

// handleLocalBatch delegates batches to remote hosts and local nodes
func (h *host) handleBatch(op uint8, batch entity.Batch) (res []uint8, err error) {
	if !h.active {
		err = fmt.Errorf("Starting up")
		return
	}
	var wg sync.WaitGroup
	var mutex sync.RWMutex
	var processed int
	var delegationPlan entity.BatchPlan
	delegationPlan, err = h.PlanDelegation(batch)
	if err != nil {
		h.log.Errorf(err.Error())
		return
	}
	res = make([]uint8, len(batch))
	for hostID, planSegment := range delegationPlan {
		if hostID == h.id {
			continue
		}
		wg.Add(1)
		go func(addr string, batch entity.Batch) {
			// Delegate
			delResp, err := h.svcDelegation.Delegate(op, addr, batch)
			if err != nil {
				h.log.Errorf(err.Error())
			}
			// Mark result
			mutex.Lock()
			defer mutex.Unlock()
			for i, item := range batch {
				res[item.N] = delResp[i]
				processed++
			}
			wg.Done()
		}(planSegment.NodeAddr, planSegment.Batch)
	}
	if _, ok := delegationPlan[h.id]; ok {
		// Persist
		locResp, err := h.handleLocalBatch(op, delegationPlan[h.id].Batch)
		if err != nil {
			h.log.Errorf(err.Error())
		}
		// Mark result
		mutex.Lock()
		for i, item := range delegationPlan[h.id].Batch {
			res[item.N] = locResp[i]
			processed++
		}
		mutex.Unlock()
	}
	wg.Wait()
	if processed < len(batch) {
		err = fmt.Errorf("Only processed %d out of %d items", processed, len(batch))
	}
	return
}

// Receives GRPC call for delegation
func (h *host) Delegate(ctx context.Context, req *grpcint.BatchOp) (reply *grpcint.BatchReply, err error) {
	if !h.active {
		err = fmt.Errorf("Starting up")
		return
	}
	var batch = make(entity.Batch, len(req.Items)/h.keylen)
	var hash []byte
	for i := 0; i < len(batch); i++ {
		hash = req.Items[i*h.keylen : i*h.keylen+h.keylen]
		batch[i] = entity.BatchItem{
			N:         i,
			Hash:      hash,
			Partition: h.svcHash.GetPartition(hash),
		}
	}
	res, err := h.handleLocalBatch(uint8(req.Op), batch)
	if err != nil {
		h.log.Errorf(err.Error())
		return
	}
	reply = &grpcint.BatchReply{}
	reply.Res = make([]byte, len(batch))
	for i := range res {
		reply.Res[i] = byte(res[i])
	}
	return
}

// handleLocalBatch distributes batches to local nodes
func (h *host) handleLocalBatch(op uint8, batch entity.Batch) (res []uint8, err error) {
	var batches = map[uint64]entity.Batch{}
	var node *entity.Node
	var p []byte
	for _, item := range batch {
		node = h.manifest.Catalog.GetLocalNodeByPartition(item.Partition, h.id)
		if node == nil {
			err = fmt.Errorf("Node not found for partition: %04x", p)
			return
		}
		if _, ok := batches[node.ID]; !ok {
			batches[node.ID] = entity.Batch{}
		}
		batches[node.ID] = append(batches[node.ID], item)
	}
	res = make([]uint8, len(batch))
	var wg sync.WaitGroup
	for nodeID, batch := range batches {
		wg.Add(1)
		go func(nodeID uint64, batch entity.Batch) {
			defer wg.Done()
			node, ok := h.nodes[nodeID]
			if !ok {
				h.log.Errorf("Unrecognized node (%d)", nodeID)
				return
			}
			res2, err2 := node.HandleBatch(uint8(op), batch)
			if err2 != nil {
				err = err2
				h.log.Errorf(err.Error())
				return
			}
			for i, item := range batch {
				res[item.N] = byte(res2[i])
			}
		}(nodeID, batch)
	}
	wg.Wait()
	return
}

// GetInitChan returns the host's initialization channel
func (h *host) GetInitChan() chan entity.Catalog {
	return h.initChan
}

func containsUint64(src []uint64, tgt uint64) bool {
	for _, i := range src {
		if i == tgt {
			return true
		}
	}
	return false
}

// PlanDelegation returns a batch delegation plan
// Batches are delegated to partition master nodes
func (h *host) PlanDelegation(batch entity.Batch) (plan entity.BatchPlan, err error) {
	plan = entity.BatchPlan{}
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	var hostMap = h.manifest.Catalog.GetPartitionMap()
	for clusterID, b := range batch.Partitioned() {
		// TODO - rotate host based on retry number
		// TODO - rotate host based on circuit breaker failure
		var hostID = hostMap[clusterID][0]
		if hostID != h.id && containsUint64(hostMap[clusterID], h.id) {
			hostID = h.id
		}
		if _, ok := plan[hostID]; !ok {
			host := h.manifest.GetHostByID(hostID)
			plan[hostID] = &entity.BatchPlanSegment{
				NodeAddr: host.IntApiAddr,
				Batch:    entity.Batch{},
			}
		}
		plan[hostID].Batch = append(plan[hostID].Batch, b...)
	}
	return
}

// Start starts the service
func (h *host) Start() (err error) {
	go func() {
		for {
			select {
			case cat, ok := <-h.initChan:
				if !ok {
					return
				}
				err := h.svcPersistence.Init(cat, h.id)
				if err != nil {
					panic(err.Error())
					return
				}
				h.active = true
			}
		}
	}()
	h.svcDelegation.Start()
	h.svcPersistence.Start()
	var initialMembers = map[uint64]string{}
	if len(h.cfg.Join) > 0 {
		for _, host := range h.cfg.Join {
			initialMembers[host.ID] = host.RaftAddr
		}
	}
	clusterConfig := dbconf.Config{
		NodeID:             h.id,
		ClusterID:          metaClusterID,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
	}
	err = h.nodeHost.StartCluster(initialMembers, !h.cfg.RaftSolo, h.factory, clusterConfig)
	if err != nil {
		return
	}
	return
}

// LeaderUpdated receives leader promotion notifications
func (h *host) LeaderUpdated(info dbio.LeaderInfo) {
	if h.leaderID != info.LeaderID {
		rs, err := h.nodeHost.ReadIndex(metaClusterID, raftTimeout)
		if err != nil {
			return
		}
		<-rs.CompletedC
		h.leaderID = info.LeaderID
	}
	if !h.active && !h.starting {
		h.init()
	}
}

func (h *host) isLeader() bool {
	return h.leaderID == h.id
}

// init is idempotent. It is called upon any state change event before the host is active and executed serially.
func (h *host) init() {
	if h.active {
		return
	}
	h.starting = true
	h.initmutex.Lock()
	defer h.initmutex.Unlock()
	defer func() { h.starting = false }()
	// Initialize manifest
	if h.manifest.Version == 0 && h.isLeader() {
		h.initializeManifest()
		return
	}

	// Update host meta if necessary
	updated, err := h.updateMeta()
	if err != nil {
		h.log.Errorf(err.Error())
		return
	}
	if updated {
		return
	}

	h.log.Printf("init: %s", string( h.manifest.ToJson()))

	// Stop if any nodes have not yet reported meta
	for _, host := range h.manifest.Catalog.Hosts {
		if host.IntApiAddr == "" {
			return
		}
	}

	h.log.Println("okay")

	h.active = true
	// Stop if any nodes have not yet reported meta
	// Schedule allocation
	// Perform allocation
	// Stop if any nodes have not yet confirmed allocation
	// Activate Cluster
	// Set status to active

}

// Update satisfies statemachine.IStateMachine
// It receives protocol buffer messages and propagates them to handlers.
func (h *host) Update(msg []byte) (reply dbsm.Result, err error) {
	if len(msg) < 2 {
		err = fmt.Errorf("Invalid Event (min len 2)")
		return
	}
	// version := msg[0] // reserved for versioning
	switch uint8(msg[1]) {
	case EVENT_TYPE_INIT:
		if h.manifest.Catalog.Version == "" {
			h.manifest.FromJson(msg[2:])
		} else {
			h.log.Debugf("Ignoring duplicate manifest initialization")
		}
	case EVENT_TYPE_TICK:
		e := event.Tick{}
		proto.Unmarshal(msg[2:], &e)
		err = h.handleTick(e)
	case EVENT_TYPE_HOST_META_UPDATE:
		e := event.HostMetaUpdate{}
		proto.Unmarshal(msg[2:], &e)
		err = h.handleMetaUpdate(e)
	case EVENT_TYPE_HOST_STATUS_UPDATE:
		e := event.HostStatusUpdate{}
		proto.Unmarshal(msg[2:], &e)
		err = h.handleStatusUpdate(e)
	default:
		err = fmt.Errorf("Unrecognized event type (%d)", uint8(msg[1]))
	}
	if err == nil {
		reply.Value = EVENT_RESPONSE_SUCCESS
	} else {
		reply.Value = EVENT_RESPONSE_FAILURE
	}
	return
}

func (h *host) wrapMsg(etype uint8, msg []byte) []byte {
	return append([]byte{EVENT_SCHEMA_VERSION, etype}, msg...)
}

// Convenience method that wraps SyncPropose w/ session acquisition
// May be overkill. Have to study expected dragonboat session management.
func (h *host) syncPropose(msg []byte) (res dbsm.Result, err error) {
	ctx, _ := context.WithTimeout(context.Background(), raftTimeout)
	sess, err := h.nodeHost.SyncGetSession(ctx, metaClusterID)
	if err != nil {
		h.log.Errorf("Error getting meta session: %s", err.Error())
		return
	}
	ctx, _ = context.WithTimeout(context.Background(), raftTimeout)
	return h.nodeHost.SyncPropose(ctx, sess, msg)
}

// updateMeta returns false if meta is up to date and true otherwise, proposing host meta data update.
func (h *host) updateMeta() (updated bool, err error) {
	databases := h.svcPersistence.GetDatabases()
	host := h.manifest.GetHostByID(h.id)
	if host == nil {
		err = fmt.Errorf("Unknown host %d", h.id)
		return
	}
	if host.IntApiAddr == h.cfg.IntApiAddr &&
		reflect.DeepEqual(host.Databases, databases) &&
		reflect.DeepEqual(config.HostMeta(host.Meta), h.cfg.Meta) {
		return false, nil
	}
	e := event.HostMetaUpdate{
		Id:         h.id,
		Meta:       h.cfg.Meta,
		Databases:  databases,
		IntApiAddr: h.cfg.IntApiAddr,
	}
	msg, err := proto.Marshal(&e)
	if err != nil {
		return
	}
	_, err = h.syncPropose(h.wrapMsg(EVENT_TYPE_HOST_META_UPDATE, msg))
	if err != nil {
		return
	}
	return true, nil
}

// handleHostMetaUpdate handles host meta update events
func (h *host) handleMetaUpdate(e event.HostMetaUpdate) (err error) {
	host := h.manifest.GetHostByID(e.Id)
	if host == nil {
		err = fmt.Errorf("Unknown host %d", e.Id)
		h.log.Errorf(err.Error())
		return
	}
	host.Meta = e.Meta
	host.Databases = e.Databases
	host.IntApiAddr = e.IntApiAddr
	return
}

// updateStatus proposes node status update to deployment leader
func (h *host) updateStatus(status uint32) (err error) {
	e := event.HostStatusUpdate{
		Id:     h.id,
		Status: status,
	}
	msg, err := proto.Marshal(&e)
	if err != nil {
		return
	}
	_, err = h.syncPropose(h.wrapMsg(EVENT_TYPE_HOST_STATUS_UPDATE, msg))
	return
}

// handleHostStatusUpdate handles host status update events
func (h *host) handleStatusUpdate(e event.HostStatusUpdate) (err error) {
	host := h.manifest.GetHostByID(e.Id)
	if host == nil {
		err = fmt.Errorf("Unknown host %d", e.Id)
		h.log.Errorf(err.Error())
		return
	}
	host.Status = uint8(e.Status)
	return
}

// updateEpoch proposes a new epoch
func (h *host) updateEpoch(epoch uint32) (err error) {
	e := event.Tick{
		Epoch: epoch,
	}
	msg, err := proto.Marshal(&e)
	if err != nil {
		return
	}
	_, err = h.syncPropose(h.wrapMsg(EVENT_TYPE_TICK, msg))
	return
}

// handleTick handles tick events from deployment leader
func (h *host) handleTick(e event.Tick) (err error) {
	h.manifest.Epoch = e.Epoch
	// Notify all local clusters of epoch
	return
}

// Lookup satisfies statemachine.IStateMachine
func (h *host) Lookup(interface{}) (res interface{}, err error) {
	h.log.Debugf("Lookup")
	return
}

// SaveSnapshot satisfies statemachine.IStateMachine
func (h *host) SaveSnapshot(w io.Writer, c dbsm.ISnapshotFileCollection, d <-chan struct{}) (err error) {
	w.Write(h.manifest.ToJson())
	h.log.Debugf("SaveSnapshot")
	return
}

// RecoverFromSnapshot satisfies statemachine.IStateMachine
func (h *host) RecoverFromSnapshot(r io.Reader, c []dbsm.SnapshotFile, d <-chan struct{}) (err error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	h.manifest.FromJson(data)
	h.log.Debugf("RecoverFromSnapshot")
	return
}

// Close satisfies statemachine.IStateMachine
func (h *host) Close() (err error) {
	return
}

// Stop stops services
func (h *host) Stop() {
	close(h.initChan)
	h.svcPersistence.Stop()
	h.svcDelegation.Stop()
}

// Build initial manifest if none exists upon cluster bootstrap
func (h *host) initializeManifest() (err error) {
	if h.manifest.Catalog.Version != "" {
		return
	}
	var hosts = make([]entity.Host, len(h.cfg.Join))
	for i, join := range h.cfg.Join {
		var host = entity.Host{
			ID:       join.ID,
			RaftAddr: join.RaftAddr,
			Status:   entity.HOST_STATUS_INITIALIZING,
		}
		if h.id == host.ID {
			host.IntApiAddr = h.cfg.IntApiAddr
			host.Meta = h.cfg.Meta
		}
		hosts[i] = host
	}
	h.manifest = &entity.Manifest{
		Version:      1,
		DeploymentID: h.cfg.DeploymentID,
		Status:       entity.DEPLOYMENT_STATUS_INITIALIZING,
		Catalog: entity.Catalog{
			Version:      "1.0.0",
			ReplicaCount: h.cfg.ReplicaCount,
			WitnessCount: h.cfg.WitnessCount,
			Hosts:        hosts,
		},
	}
	_, err = h.syncPropose(h.wrapMsg(EVENT_TYPE_INIT, h.manifest.ToJson()))
	if err != nil {
		h.log.Errorf("Error proposing initial manifest: %s", err.Error())
		return
	}
	h.log.Debugf("%d Manifest Initialized", h.id)

	return nil
}
