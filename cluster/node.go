package cluster

import (
	"context"
	"fmt"
	"sync"
	"io"

	dbsm "github.com/lni/dragonboat/v3/statemachine"

	"highvolume.io/shackle/api/grpcint"
	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/log"
	"highvolume.io/shackle/service"
)

type Node interface {
	Lock(entity.Batch) ([]uint8, error)
	Commit(entity.Batch) ([]uint8, error)
	Rollback(entity.Batch) ([]uint8, error)
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
	hostID          uint64
	keylen          int
	log             log.Logger
	svcHash         service.Hash
	svcCoordination service.Coordination
	svcPersistence  service.Persistence
	svcDelegation   service.Delegation
	initChan        chan entity.Catalog
	activeChan      chan bool
	active          bool
}

// NewNode returns a new node
func NewNode(
	cfg config.App,
	log log.Logger,
	svcHash service.Hash,
	svcCoordination service.Coordination,
	svcPersistence service.Persistence,
	svcDelegation service.Delegation,
	initChan chan entity.Catalog,
) (*node, error) {
	return &node{
		cfg.Host.ID,
		cfg.Host.KeyLength,
		log,
		svcHash,
		svcCoordination,
		svcPersistence,
		svcDelegation,
		initChan,
		make(chan bool),
		false,
	}, nil
}

// Lock locks a batch for processing
func (n *node) Lock(batch entity.Batch) (res []uint8, err error) {
	return n.handleBatch(entity.OP_LOCK, batch)
}

// Commit commits a previously locked batch
func (n *node) Commit(batch entity.Batch) (res []uint8, err error) {
	return n.handleBatch(entity.OP_COMMIT, batch)
}

// Rollback rolls back a previously locked batch
func (n *node) Rollback(batch entity.Batch) (res []uint8, err error) {
	return n.handleBatch(entity.OP_ROLLBACK, batch)
}

func (n *node) handleBatch(op uint8, batch entity.Batch) (res []uint8, err error) {
	if !n.active {
		err = fmt.Errorf("Starting up")
		return
	}
	var wg sync.WaitGroup
	var mutex sync.RWMutex
	var processed int
	var delegationPlan entity.BatchPlan
	delegationPlan, err = n.svcCoordination.PlanDelegation(batch)
	if err != nil {
		n.log.Errorf(err.Error())
		return
	}
	res = make([]uint8, len(batch))
	for k, v := range delegationPlan {
		if k == n.hostID {
			continue
		}
		wg.Add(1)
		go func(nodeid uint64, addr string, batch entity.Batch) {
			// Delegate
			delResp, err := n.svcDelegation.Delegate(op, addr, batch)
			if err != nil {
				n.log.Errorf(err.Error())
			}
			// Mark result
			mutex.Lock()
			defer mutex.Unlock()
			for i, item := range batch {
				res[item.N] = delResp[i]
				processed++
			}
			wg.Done()
		}(k, v.NodeAddr, v.Batch)
	}
	if _, ok := delegationPlan[n.hostID]; ok {
		// Persist
		persistResp, err := n.handlePersist(op, delegationPlan[n.hostID].Batch)
		if err != nil {
			n.log.Errorf(err.Error())
		}
		// Mark result
		mutex.Lock()
		for i, item := range delegationPlan[n.hostID].Batch {
			res[item.N] = persistResp[i]
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
func (n *node) Delegate(ctx context.Context, req *grpcint.BatchOp) (reply *grpcint.BatchReply, err error) {
	if !n.active {
		err = fmt.Errorf("Starting up")
		return
	}
	batch := make(entity.Batch, len(req.Items)/n.keylen)
	var hash = make([]byte, n.keylen)
	for i := 0; i < len(batch); i++ {
		hash = req.Items[i*n.keylen : i*n.keylen+n.keylen]
		batch[i] = entity.BatchItem{
			N:         i,
			Hash:      hash,
			Partition: n.svcHash.GetPartition(hash),
		}
	}

	res, err := n.handleBatch(uint8(req.Op), batch)

	reply = &grpcint.BatchReply{}
	if err != nil {
		reply.Err = err.Error()
	}
	reply.Res = make([]byte, len(batch))
	for i, b := range res {
		reply.Res[i] = byte(b)
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
	n.log.Debugf("Lookup")
	return
}
func (n *node) SaveSnapshot(interface {}, io.Writer, <-chan struct {}) (err error) {
	n.log.Debugf("SaveSnapshot")
	return
}
func (n *node) RecoverFromSnapshot(io.Reader, <-chan struct{}) (err error) {
	n.log.Debugf("RecoverFromSnapshot")
	return
}

// Start starts services and initchan
func (n *node) Start() {
	go func() {
		for {
			select {
			case cat, ok := <-n.initChan:
				if !ok {
					return
				}
				err := n.svcPersistence.Init(cat, n.hostID)
				if err != nil {
					panic(err.Error())
					return
				}
				n.active = true
				close(n.activeChan)
			}
		}
	}()
	n.svcDelegation.Start()
	n.svcPersistence.Start()
	n.svcCoordination.Start()
}

// Close stops node
func (n *node) Close() (err error) {
	return
}

// Stop stops services
func (n *node) Stop() {
	n.svcCoordination.Stop()
	n.svcPersistence.Stop()
	n.svcDelegation.Stop()
}
