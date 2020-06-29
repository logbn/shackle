package cluster

import (
	"context"
	"fmt"
	"sync"

	"highvolume.io/shackle/api/intapi"
	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/log"
	"highvolume.io/shackle/service"
)

type Node interface {
	Lock(entity.Batch) ([]int8, error)
	Commit(entity.Batch) ([]int8, error)
	Rollback(entity.Batch) ([]int8, error)
	Active() chan bool
	Start()
	Stop()
}

type node struct {
	id              string
	keylen          int
	log             log.Logger
	svcHash         service.Hash
	svcCoordination service.Coordination
	svcPersistence  service.Persistence
	svcReplication  service.Replication
	svcDelegation   service.Delegation
	initChan        chan entity.ClusterCatalog
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
	svcReplication service.Replication,
	svcDelegation service.Delegation,
	initChan chan entity.ClusterCatalog,
) (*node, error) {
	return &node{
		cfg.Cluster.Node.ID,
		cfg.Cluster.KeyLength,
		log,
		svcHash,
		svcCoordination,
		svcPersistence,
		svcReplication,
		svcDelegation,
		initChan,
		make(chan bool),
		false,
	}, nil
}

// Lock locks a batch for processing
func (n *node) Lock(batch entity.Batch) (res []int8, err error) {
	return n.handleBatch(entity.OP_LOCK, batch)
}

// Commit commits a previously locked batch
func (n *node) Commit(batch entity.Batch) (res []int8, err error) {
	return n.handleBatch(entity.OP_COMMIT, batch)
}

// Rollback rolls back a previously locked batch
func (n *node) Rollback(batch entity.Batch) (res []int8, err error) {
	return n.handleBatch(entity.OP_ROLLBACK, batch)
}

func (n *node) handleBatch(op uint32, batch entity.Batch) (res []int8, err error) {
	if !n.active {
		err = fmt.Errorf("Starting up")
		return
	}
	var wg sync.WaitGroup
	var mutex sync.RWMutex
	var processed int
	var delegationPlan entity.BatchPlan
	var replicationPlan entity.BatchPlan
	delegationPlan, err = n.svcCoordination.PlanDelegation(batch)
	if err != nil {
		n.log.Errorf(err.Error())
		return
	}
	res = make([]int8, len(batch))
	for k, v := range delegationPlan {
		if k == n.id {
			continue
		}
		wg.Add(1)
		go func(nodeid string, addr string, batch entity.Batch) {
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
	if _, ok := delegationPlan[n.id]; ok {
		// Persist
		persistResp, err := n.handlePersist(op, delegationPlan[n.id].Batch)
		if err != nil {
			n.log.Errorf(err.Error())
		}
		// Mark result
		mutex.Lock()
		for i, item := range delegationPlan[n.id].Batch {
			res[item.N] = persistResp[i]
			processed++
		}
		mutex.Unlock()
		replicationPlan, err = n.svcCoordination.PlanReplication(delegationPlan[n.id].Batch)
		for nodeid, planSegment := range replicationPlan {
			wg.Add(1)
			go func(nodeid string, addr string, replicationBatch entity.Batch) {
				// Replicate
				replicationResp, err := n.svcReplication.Replicate(op, addr, replicationBatch)
				if err != nil {
					n.log.Errorf(err.Error())
				}
				mutex.RLock()
				// Parity check against master result
				var origN int
				for i, item := range replicationBatch {
					origN = delegationPlan[n.id].Batch[item.N].N
					if res[origN] != replicationResp[i] {
						n.log.Warnf("Differing response master %s=%d, replica %s=%d", n.id, res[origN],
							nodeid, replicationResp[i])
					}
				}
				mutex.RUnlock()
				wg.Done()
			}(nodeid, planSegment.NodeAddr, planSegment.Batch)
		}
	}
	wg.Wait()
	if processed < len(batch) {
		err = fmt.Errorf("Only processed %d out of %d items", processed, len(batch))
	}
	return
}

// Receives GRPC call for delegation
func (n *node) Delegate(ctx context.Context, req *intapi.BatchOp) (reply *intapi.BatchReply, err error) {
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

	res, err := n.handleBatch(req.Op, batch)

	reply = &intapi.BatchReply{}
	if err != nil {
		reply.Err = err.Error()
	}
	reply.Res = make([]byte, len(batch))
	for i, b := range res {
		reply.Res[i] = byte(b)
	}
	return
}

// Receives GRPC call for replication
func (n *node) Replicate(ctx context.Context, req *intapi.BatchOp) (reply *intapi.BatchReply, err error) {
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

	// Persist
	res, err := n.handlePersist(req.Op, batch)
	if err != nil {
		n.log.Errorf(err.Error())
	}

	// Reply
	reply = &intapi.BatchReply{}
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
func (n *node) handlePersist(op uint32, batch entity.Batch) (res []int8, err error) {
	res = make([]int8, len(batch))
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

// Start starts services and initchan
func (n *node) Start() {
	go func() {
		for {
			select {
			case cat, ok := <-n.initChan:
				if !ok {
					return
				}
				err := n.svcPersistence.Init(cat, n.id)
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
	n.svcReplication.Start()
	n.svcCoordination.Start()
}

// Stop stops services
func (n *node) Stop() {
	n.svcCoordination.Stop()
	n.svcPersistence.Stop()
	n.svcReplication.Stop()
	n.svcDelegation.Stop()
}

// Active returns a channel that can be used to block the caller until the node is active
func (n *node) Active() chan bool {
	return n.activeChan
}
