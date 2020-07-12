package cluster

import (
	"fmt"
	"io"

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
		cfg,
		log,
		0,
		0,
		partition,
		cfg.KeyLength,
		svcHash,
		svcPersistence,
		make(chan bool),
		true,
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
	n.svcPersistence.InitRepo(n.Partition)
	n.log.Debugf("[Host %d] Open %04x", n.cfg.ID, n.Partition)
	return
}
func (n *node) Sync() (err error) {
	// n.log.Debugf("Sync")
	return
}
func (n *node) PrepareSnapshot() (res interface{}, err error) {
	// n.log.Debugf("PrepareSnapshot")
	return
}
func (n *node) Update(ents []dbsm.Entry) ([]dbsm.Entry, error) {
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
