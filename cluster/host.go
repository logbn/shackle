package cluster

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/v3"
	dbclient "github.com/lni/dragonboat/v3/client"
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

	EVENT_TYPE_NULL                     uint8 = 0
	EVENT_TYPE_INIT                     uint8 = 1
	EVENT_TYPE_TICK                     uint8 = 2
	EVENT_TYPE_HOST_META_UPDATE         uint8 = 3
	EVENT_TYPE_HOST_STATUS_UPDATE       uint8 = 4
	EVENT_TYPE_ALLOCATE                 uint8 = 5
	EVENT_TYPE_DEPLOYMENT_STATUS_UPDATE uint8 = 6

	EVENT_RESPONSE_FAILURE uint64 = 0
	EVENT_RESPONSE_SUCCESS uint64 = 1

	metaClusterID   = math.MaxUint64
	raftTimeout     = 5 * time.Second
	rttMilliseconds = 200
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
	active         bool
	starting       bool
	keylen         int
	factory        func(uint64, uint64) dbsm.IStateMachine
	nodes          map[uint64]Node
	metaSession    *dbclient.Session
}

// NewHost returns a new host
func NewHost(
	cfg config.Host,
	log log.Logger,
	svcHash service.Hash,
	svcPersistence service.Persistence,
	svcDelegation service.Delegation,
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
		keylen:         cfg.KeyLength,
		clock:          clock.NewMock(),
		nodes:          make(map[uint64]Node),
	}
	h.factory = func(clusterID uint64, nodeID uint64) dbsm.IStateMachine {
		h.clusterID = clusterID
		h.nodeID = nodeID
		return h
	}
	level := dblog.WARNING
	// level := dblog.INFO
	dblog.GetLogger("dragonboat").SetLevel(level)
	dblog.GetLogger("transport").SetLevel(level)
	dblog.GetLogger("logdb").SetLevel(level)
	dblog.GetLogger("raft").SetLevel(level)
	dblog.GetLogger("grpc").SetLevel(level)
	dblog.GetLogger("rsm").SetLevel(level)
	h.nodeHost, err = dragonboat.NewNodeHost(dbconf.NodeHostConfig{
		DeploymentID:      cfg.DeploymentID,
		NodeHostDir:       cfg.RaftDir,
		RTTMillisecond:    rttMilliseconds,
		RaftAddress:       cfg.RaftAddr,
		EnableMetrics:     true,
		RaftEventListener: h,
	})
	return
}

// Start starts the service
func (h *host) Start() (err error) {
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
	go func() {
		for !h.active {
			time.Sleep(time.Second)
			if h.starting {
				h.init()
			}
		}
	}()
	return
}

// LeaderUpdated receives leader promotion notifications
func (h *host) LeaderUpdated(info dbio.LeaderInfo) {
	if info.ClusterID == metaClusterID && h.leaderID != info.LeaderID {
		rs, err := h.nodeHost.ReadIndex(metaClusterID, raftTimeout)
		if rs != nil {
			defer rs.Release()
		}
		if err != nil {
			return
		}
		<-rs.CompletedC
		h.log.Debugf("[Host %d] New Leader: %d", h.id, info.LeaderID)
		status, err := h.getStatus()
		if err != nil {
			h.log.Errorf(err.Error())
		}
		h.leaderID = info.LeaderID
		if status == entity.HOST_STATUS_ACTIVE {
			err = h.svcPersistence.Init(h.manifest.Catalog, h.id)
			if err != nil {
				h.log.Errorf("[Host %d] Persistence failed initialization: %s", h.id, err.Error())
			}
			h.startNodes(true)
			h.log.Infof("[Host %d] Active", h.id)
			h.active = true
		} else {
			h.starting = true
		}
	}
}

func (h *host) isLeader() bool {
	return h.leaderID == h.id
}

// init is idempotent. It is called upon any state change event before the host is active and executed serially.
func (h *host) init() {
	if h.active || !h.starting {
		return
	}

	// Initialize manifest
	if h.manifest.Version == 0 {
		if h.isLeader() {
			h.log.Debugf("[Host %d] Initializing Manifest", h.id)
			h.initializeManifest()
		} else {
			h.log.Debugf("[Host %d] Waiting for Manifest", h.id)
		}
		return
	}
	// Update host meta if necessary
	updated, err := h.updateMeta()
	if err != nil {
		h.log.Errorf("[Host %d] %s", h.id, err.Error())
		return
	}
	if updated {
		h.log.Debugf("[Host %d] Meta Updated", h.id)
		return
	}
	if h.isLeader() {
		// Stop if any nodes have not yet reported meta
		for _, host := range h.manifest.Catalog.Hosts {
			if host.IntApiAddr == "" {
				h.log.Errorf("[Host %d] Waiting for host meta", h.id)
				return
			}
		}
		// Schedule allocation
		if h.manifest.Initializing() {
			h.log.Debugf("[Host %d] Allocating %d partitions", h.id, h.manifest.Catalog.Partitions)
			catalog, err := h.manifest.Allocate()
			if err != nil {
				h.log.Errorf(err.Error())
				return
			}
			_, err = h.syncPropose(h.wrapMsg(EVENT_TYPE_ALLOCATE, catalog.ToJson()))
			if err != nil {
				h.log.Errorf("[Host %d] Error proposing allocation: %s", h.id, err.Error())
				return
			}
			return
		} else {
			h.log.Debugf("[Host %d] Manifest not initializing (%d)", h.id, h.manifest.Status)
		}
		// Stop if any nodes have not yet reported meta
		for _, host := range h.manifest.Catalog.Hosts {
			if !host.Allocated() {
				h.log.Errorf("[Host %d] Waiting for host %d to allocate", h.id, host.ID)
				return
			}
		}
		h.updateDeploymentStatus(entity.DEPLOYMENT_STATUS_ACTIVE)
	}

	// Activate Clusters
	host := h.manifest.GetHostByID(h.id)
	if host.Allocated() {
		if len(h.nodes) == 0 {
			err := h.startNodes(true)
			if err != nil {
				h.log.Errorf(err.Error())
			}
		} else {
			var inactive int
			for _, node := range h.nodes {
				if !node.Active() {
					inactive++
				}
			}
			if inactive > 0 {
				h.log.Errorf("[Host %d] Waiting for %d nodes to activate", h.id, inactive)
			}
		}
	}

	// Set status to active
	if h.manifest.Active() {
		err = h.updateStatus(entity.HOST_STATUS_ACTIVE)
		if err == nil {
			h.log.Infof("[Host %d] active", h.id)
			h.active = true
			h.starting = false
		}
	}
}

func (h *host) startNodes(init bool) (err error) {
	for _, nodeManifest := range h.manifest.Catalog.Nodes {
		if nodeManifest.HostID != h.id {
			continue
		}
		var peers = make(map[uint64]string)
		if init {
			peers = h.manifest.GetPartitionPeers(nodeManifest.Partition)
		}
		n, err := NewNode(h.cfg, h.log, h.svcHash, h.svcPersistence, nodeManifest.Partition)
		if err != nil {
			h.log.Errorf(err.Error())
			return err
		}
		var factory = func(clusterID uint64, nodeID uint64) dbsm.IOnDiskStateMachine {
			n.ClusterID = clusterID
			n.ID = nodeID
			return n
		}
		// println(len(peers), !(nodeManifest.IsLeader && init))
		err = h.nodeHost.StartOnDiskCluster(peers, !init, factory, dbconf.Config{
			NodeID:             h.id,
			ClusterID:          uint64(nodeManifest.ClusterID),
			ElectionRTT:        10,
			HeartbeatRTT:       1,
			CheckQuorum:        true,
			SnapshotEntries:    10,
			CompactionOverhead: 5,
		})
		if err != nil {
			h.log.Errorf(err.Error())
			return err
		}
		h.nodes[nodeManifest.ID] = n
	}
	return
}

// Update satisfies statemachine.IStateMachine
// It receives protocol buffer messages and propagates them to handlers.
//
//
//
//       IF THIS METHOD RETURNS AN ERROR, DRAGONBOAT WILL PANIC.
//
//
//
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
			h.log.Warnf("Ignoring duplicate manifest initialization")
		}
	case EVENT_TYPE_TICK:
		err = h.handleTick(msg[2:])
	case EVENT_TYPE_HOST_META_UPDATE:
		err = h.handleMetaUpdate(msg[2:])
	case EVENT_TYPE_HOST_STATUS_UPDATE:
		err = h.handleStatusUpdate(msg[2:])
	case EVENT_TYPE_DEPLOYMENT_STATUS_UPDATE:
		err = h.handleDeploymentStatusUpdate(msg[2:])
	case EVENT_TYPE_ALLOCATE:
		err = h.allocate(msg[2:])
	default:
		err = fmt.Errorf("Unrecognized event type (%d)", uint8(msg[1]))
	}
	if err == nil {
		reply.Value = EVENT_RESPONSE_SUCCESS
	} else {
		h.log.Errorf(err.Error())
		err = nil
		reply.Value = EVENT_RESPONSE_FAILURE
	}
	return
}

// Initialize vnodes. Special operation of the persistence service.
func (h *host) allocate(data []byte) (err error) {
	h.log.Debugf("[Host %d] allocate called", h.id)
	h.manifest.Status = entity.DEPLOYMENT_STATUS_ALLOCATING
	h.manifest.Catalog.FromJson(data)
	err = h.svcPersistence.Init(h.manifest.Catalog, h.id)
	if err != nil {
		h.log.Debugf("[Host %d] Not allocated %s", h.id, err.Error())
		return
	}
	h.log.Debugf("[Host %d] Allocated", h.id)
	go func() {
		err = h.updateStatus(entity.HOST_STATUS_ALLOCATED)
		if err != nil {
			h.log.Debugf("[Host %d] Error updating status %s", h.id, err.Error())
			err = h.updateStatus(entity.HOST_STATUS_ALLOCATED)
		}
	}()
	return
}

func (h *host) wrapMsg(etype uint8, msg []byte) []byte {
	return append([]byte{EVENT_SCHEMA_VERSION, etype}, msg...)
}

func (h *host) getSession(clusterID uint64) (ctx context.Context, sess *dbclient.Session, err error) {
	ctx, _ = context.WithTimeout(context.Background(), raftTimeout)
	ctx2, _ := context.WithTimeout(context.Background(), raftTimeout)
	sess, err = h.nodeHost.SyncGetSession(ctx2, clusterID)
	if err != nil {
		err = fmt.Errorf("Error getting meta session: %s", err.Error())
		return
	}
	return
}

// Convenience method that wraps SyncPropose w/ session acquisition
// May be overkill. Have to study expected dragonboat session management.
func (h *host) syncPropose(msg []byte) (res dbsm.Result, err error) {
	ctx, sess, err := h.getSession(metaClusterID)
	if err != nil {
		return
	}
	res, err = h.nodeHost.SyncPropose(ctx, sess, msg)
	sess.ProposalCompleted()
	return
}

// Convenience method that wraps SyncPropose w/ session acquisition
func (h *host) syncProposeNode(clusterID uint64, msg []byte) (res dbsm.Result, err error) {
	ctx, _ := context.WithTimeout(context.Background(), raftTimeout)
	sess := h.nodeHost.GetNoOPSession(clusterID)
	res, err = h.nodeHost.SyncPropose(ctx, sess, msg)
	return
}

// updateMeta returns false if meta is up to date and true otherwise, proposing host meta data update.
func (h *host) updateMeta() (updated bool, err error) {
	databases := h.svcPersistence.GetDatabases()
	host := h.manifest.GetHostByID(h.id)
	if host == nil {
		err = fmt.Errorf("Unknown host c %d", h.id)
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

// handleMetaUpdate handles host meta update events
func (h *host) handleMetaUpdate(msg []byte) (err error) {
	e := event.HostMetaUpdate{}
	err = proto.Unmarshal(msg, &e)
	if err != nil {
		return
	}
	host := h.manifest.GetHostByID(e.Id)
	if host == nil {
		err = fmt.Errorf("Unknown host a %d", e.Id)
		h.log.Errorf(err.Error())
		return
	}
	host.Meta = e.Meta
	host.Databases = e.Databases
	host.IntApiAddr = e.IntApiAddr
	h.log.Debugf("[Host %d] handleMetaUpdate for host %d", h.id, e.Id)
	return
}

// getStatus returns host status
func (h *host) getStatus() (status uint8, err error) {
	host := h.manifest.GetHostByID(h.id)
	if host == nil {
		err = fmt.Errorf("Unknown host a %d", h.id)
		return
	}
	status = host.Status
	return
}

// updateStatus proposes node status update to deployment leader if necessary (idempotent)
func (h *host) updateStatus(status uint8) (err error) {
	currentStatus, err := h.getStatus()
	if err != nil {
		h.log.Errorf(err.Error())
		return
	}
	if currentStatus == status {
		return
	}
	e := event.HostStatusUpdate{
		Id:     h.id,
		Status: uint32(status),
	}
	msg, err := proto.Marshal(&e)
	if err != nil {
		return
	}
	_, err = h.syncPropose(h.wrapMsg(EVENT_TYPE_HOST_STATUS_UPDATE, msg))
	return
}

// handleStatusUpdate handles host status update events
func (h *host) handleStatusUpdate(msg []byte) (err error) {
	e := event.HostStatusUpdate{}
	err = proto.Unmarshal(msg, &e)
	if err != nil {
		return
	}
	host := h.manifest.GetHostByID(e.Id)
	if host == nil {
		err = fmt.Errorf("Unknown host b %d", e.Id)
		h.log.Errorf(err.Error())
		return
	}
	host.Status = uint8(e.Status)
	h.log.Debugf("[Host %d] handleStatusUpdate for host %d: %d", h.id, e.Id, e.Status)
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
func (h *host) handleTick(msg []byte) (err error) {
	e := event.Tick{}
	proto.Unmarshal(msg, &e)
	h.manifest.Epoch = e.Epoch
	// TODO - Notify all local clusters of epoch
	h.log.Debugf("[Host %d] tick", h.id)
	return
}

// updateDeploymentStatus proposes deployment status update to deployment leader
func (h *host) updateDeploymentStatus(status uint8) (err error) {
	e := event.DeploymentStatusUpdate{
		Status: uint32(status),
	}
	msg, err := proto.Marshal(&e)
	if err != nil {
		return
	}
	_, err = h.syncPropose(h.wrapMsg(EVENT_TYPE_DEPLOYMENT_STATUS_UPDATE, msg))
	return
}

// handleDeploymentStatusUpdate handles deployment status update events
func (h *host) handleDeploymentStatusUpdate(msg []byte) (err error) {
	e := event.DeploymentStatusUpdate{}
	err = proto.Unmarshal(msg, &e)
	if err != nil {
		return
	}
	h.manifest.Status = uint8(e.Status)
	h.log.Debugf("[Host %d] handleDeploymentStatusUpdate: %d", h.id, e.Status)
	return
}

// Lookup satisfies statemachine.IStateMachine
func (h *host) Lookup(interface{}) (res interface{}, err error) {
	h.log.Debugf("Lookup")
	return
}

// SaveSnapshot satisfies statemachine.IStateMachine
func (h *host) SaveSnapshot(w io.Writer, c dbsm.ISnapshotFileCollection, d <-chan struct{}) (err error) {
	_, err = w.Write(h.manifest.ToJson())
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
	var sz int
	for i, item := range batch {
		node = h.manifest.Catalog.GetLocalNodeByPartition(item.Partition, h.id)
		if node == nil {
			err = fmt.Errorf("Node not found for partition: %04x", p)
			return
		}
		if _, ok := batches[node.ID]; !ok {
			batches[node.ID] = entity.Batch{}
		}
		sz = len(batches[node.ID])
		batches[node.ID] = append(batches[node.ID], item)
		batches[node.ID][sz].N = i
	}
	res = make([]uint8, len(batch))
	var wg sync.WaitGroup
	for nodeID, batch := range batches {
		wg.Add(1)
		go func(nodeID uint64, batch entity.Batch) {
			defer wg.Done()
			node, ok := h.nodes[nodeID]
			if !ok {
				h.log.Errorf("Unrecognized node (%d) %d", nodeID, len(h.nodes))
				return
			}
			res2, err2 := h.syncProposeNode(node.GetClusterID(), batch.ToBytes(op))
			if err2 != nil {
				err = err2
				h.log.Errorf(err.Error())
				return
			}
			if len(res2.Data) < len(batch) {
				h.log.Errorf("Incorrect batch response length: %d != %d", len(res2.Data), len(batch))
				return
			}
			for i, item := range batch {
				res[item.N] = byte(res2.Data[i])
			}
		}(nodeID, batch)
	}
	wg.Wait()
	return
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

// Close satisfies statemachine.IStateMachine
func (h *host) Close() (err error) {
	return
}

// Stop stops services
func (h *host) Stop() {
	h.svcPersistence.Stop()
	h.svcDelegation.Stop()
}

// Build initial manifest if none exists upon cluster bootstrap
func (h *host) initializeManifest() (err error) {
	if h.manifest.Version > 0 {
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), raftTimeout)
	membership, err := h.nodeHost.SyncGetClusterMembership(ctx, metaClusterID)
	if err != nil {
		return
	}
	var hosts []entity.Host
	for id, raftAddr := range membership.Nodes {
		var host = entity.Host{
			ID:       id,
			RaftAddr: raftAddr,
			Status:   entity.HOST_STATUS_INITIALIZING,
		}
		if h.id == id {
			host.IntApiAddr = h.cfg.IntApiAddr
			host.Meta = h.cfg.Meta
		}
		hosts = append(hosts, host)
	}
	manifest := &entity.Manifest{
		Version:      1,
		DeploymentID: h.cfg.DeploymentID,
		Status:       entity.DEPLOYMENT_STATUS_INITIALIZING,
		Catalog: entity.Catalog{
			Version:      "1.0.0",
			KeyLength:    uint8(h.cfg.KeyLength),
			Partitions:   h.cfg.Partitions,
			ReplicaCount: h.cfg.ReplicaCount,
			WitnessCount: h.cfg.WitnessCount,
			Hosts:        hosts,
			Vary:         h.cfg.Vary,
		},
	}
	_, err = h.syncPropose(h.wrapMsg(EVENT_TYPE_INIT, manifest.ToJson()))
	if err != nil {
		h.log.Errorf("Error proposing initial manifest: %s", err.Error())
		return
	}
	h.manifest = manifest
	h.log.Debugf("%d Manifest Initialized", h.id)

	return nil
}
