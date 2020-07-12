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
	ID             uint64
	ClusterID      uint64
	keylen         int
	log            log.Logger
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
) (*node, error) {
	n := &node{
		0,
		0,
		cfg.KeyLength,
		log,
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

func (n *node) Active() (bool) {
	return n.active
}
func (n *node) Open(stopc <-chan struct{}) (res uint64, err error) {
	n.log.Debugf("Open")
	return
}
func (n *node) Sync() (err error) {
	n.log.Debugf("Sync")
	return
}
func (n *node) PrepareSnapshot() (res interface{}, err error) {
	n.log.Debugf("PrepareSnapshot")
	return
}
func (n *node) Update([]dbsm.Entry) (res []dbsm.Entry, err error) {
	n.log.Debugf("Update")
	return
}
func (n *node) Lookup(interface{}) (res interface{}, err error) {
	n.log.Debugf("Lookup (peek)")
	return
}
func (n *node) SaveSnapshot(interface{}, io.Writer, <-chan struct{}) (err error) {
	n.log.Debugf("SaveSnapshot")
	return
}
func (n *node) RecoverFromSnapshot(io.Reader, <-chan struct{}) (err error) {
	n.log.Debugf("RecoverFromSnapshot")
	return
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
