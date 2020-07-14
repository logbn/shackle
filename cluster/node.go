package cluster

import (
	"fmt"
	"io"
	"sync"

	dbsm "github.com/lni/dragonboat/v3/statemachine"

	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/log"
	"highvolume.io/shackle/service"
)

type Node interface {
	HandleBatch(op uint8, batch entity.Batch) (res []uint8, err error)
	GetClusterID() uint64
	GetPartition() uint16
	Active() bool
	Start()
	Stop()

	Open(stopc <-chan struct{}) (uint64, error)
	Update([]dbsm.Entry) ([]dbsm.Entry, error)
	Lookup(interface{}) (interface{}, error)
	Sync() error
	PrepareSnapshot() (interface{}, error)
	SaveSnapshot(interface{}, io.Writer, <-chan struct{}) error
	RecoverFromSnapshot(io.Reader, <-chan struct{}) error
	Close() error
}

type node struct {
	cfg            config.Host
	log            log.Logger
	ID             uint64
	ClusterID      uint64
	Partition      uint16
	keylen         int
	svcHash        service.Hash
	svcPersistence service.Persistence
	activeChan     chan bool
	active         bool

	metricMutex sync.Mutex
	batches     int
	updates     int
	items       int
}

// NewNode returns a new node
func NewNode(
	cfg config.Host,
	log log.Logger,
	svcHash service.Hash,
	svcPersistence service.Persistence,
	partition uint16,
) (*node, error) {
	n := &node{
		cfg:            cfg,
		log:            log,
		Partition:      partition,
		keylen:         cfg.KeyLength,
		svcHash:        svcHash,
		svcPersistence: svcPersistence,
		activeChan:     make(chan bool),
		active:         true,
	}
	return n, nil
}

func (n *node) HandleBatch(op uint8, batch entity.Batch) (res []uint8, err error) {
	if !n.active {
		err = fmt.Errorf("Starting up")
		return
	}
	// Persist
	res, err = n.handlePersist(op, batch)
	if err != nil {
		n.log.Errorf(err.Error())
	}
	return
}

// Extracted to reduce code duplication
func (n *node) handlePersist(op uint8, batch entity.Batch) (res []uint8, err error) {
	res = make([]uint8, len(batch))
	switch op {
	case entity.OP_LOCK:
		res, err = n.svcPersistence.Lock(batch)
	case entity.OP_COMMIT:
		res, err = n.svcPersistence.Commit(batch)
	case entity.OP_ROLLBACK:
		res, err = n.svcPersistence.Rollback(batch)
	default:
		err = fmt.Errorf("Unrecognized operation %d", op)
	}
	return
}

func (n *node) Active() bool {
	return n.active
}
func (n *node) Open(stopc <-chan struct{}) (res uint64, err error) {
	err = n.svcPersistence.InitRepo(n.Partition)
	if err != nil {
		return
	}
	n.log.Debugf("[Host %d] Open %04x", n.cfg.ID, n.Partition)
	return
}
func (n *node) Sync() (err error) {
	// n.svcPersistence.SyncRepo(n.Partition)
	n.metricMutex.Lock()
	defer n.metricMutex.Unlock()
	var avgBatches = float64(n.batches) / float64(n.updates)
	var avgBatchSize = float64(n.items) / float64(n.batches)
	n.updates = 0
	n.batches = 0
	n.items = 0
	n.log.Debugf("[Node %d] Sync %04x (batches: %.2f, batchsz: %.2f)", n.ID, n.Partition, avgBatches, avgBatchSize)
	return
}
func (n *node) PrepareSnapshot() (res interface{}, err error) {
	// n.log.Debugf("PrepareSnapshot")
	return
}
func (n *node) Update(ents []dbsm.Entry) ([]dbsm.Entry, error) {
	var multi = true
	n.metricMutex.Lock()
	defer n.metricMutex.Unlock()
	n.updates++
	n.batches += len(ents)
	if multi {
		var batches = make([]entity.Batch, len(ents))
		var ops = make([]uint8, len(ents))
		var err error
		for i, e := range ents {
			if len(e.Cmd) == 0 {
				continue
			}
			batches[i], err = entity.BatchFromBytes(e.Cmd[1:], n.keylen, n.svcHash)
			if err != nil {
				n.log.Errorf(err.Error())
			}
			ops[i] = e.Cmd[0]
			n.items += len(batches[i])
		}
		res, err := n.svcPersistence.MultiExec(n.Partition, ops, batches)
		if err != nil {
			n.log.Errorf(err.Error())
			return ents, err
		}
		for i := range res {
			ents[i].Result = dbsm.Result{Data: res[i]}
		}
	} else {
		var batch entity.Batch
		var err error
		for i, e := range ents {
			batch, err = entity.BatchFromBytes(e.Cmd[1:], n.keylen, n.svcHash)
			if err != nil {
				n.log.Errorf(err.Error())
			}
			res, err := n.handlePersist(uint8(e.Cmd[0]), batch)
			if err != nil {
				n.log.Errorf(err.Error())
			}
			ents[i].Result = dbsm.Result{Data: res}
			n.items += len(batch)
		}
	}
	// n.log.Debugf("Update")
	return ents, nil
}
func (n *node) Lookup(interface{}) (res interface{}, err error) {
	n.log.Debugf("Lookup (peek)")
	return
}
func (n *node) SaveSnapshot(interface{}, io.Writer, <-chan struct{}) (err error) {
	// n.log.Debugf("SaveSnapshot")
	return
}
func (n *node) RecoverFromSnapshot(io.Reader, <-chan struct{}) (err error) {
	n.log.Debugf("RecoverFromSnapshot")
	return
}
func (n *node) GetClusterID() uint64 {
	return n.ClusterID
}
func (n *node) GetPartition() uint16 {
	return n.Partition
}

// Start starts services and initchan
func (n *node) Start() {
}

// Close stops node
func (n *node) Close() (err error) {
	return
}

// Stop stops services
func (n *node) Stop() {
}
