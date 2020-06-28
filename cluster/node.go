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
	svcPropagation  service.Propagation
	svcDelegation   service.Delegation
	initChan        chan entity.ClusterCatalog
	active          bool
}

// NewNode returns a new node
func NewNode(
	cfg config.App,
	log log.Logger,
	svcHash service.Hash,
	svcCoordination service.Coordination,
	svcPersistence service.Persistence,
	svcPropagation service.Propagation,
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
		svcPropagation,
		svcDelegation,
		initChan,
		false,
	}, nil
}

func (n *node) Lock(batch entity.Batch) (res []int8, err error) {
	return n.handleBatch(entity.OP_LOCK, batch)
}

func (n *node) Commit(batch entity.Batch) (res []int8, err error) {
	return n.handleBatch(entity.OP_COMMIT, batch)
}

func (n *node) Rollback(batch entity.Batch) (res []int8, err error) {
	return n.handleBatch(entity.OP_ROLLBACK, batch)
}

func (n *node) handleBatch(op uint32, batch entity.Batch) (res []int8, err error) {
	if !n.active {
		err = fmt.Errorf("Starting up")
		return
	}
	res = make([]int8, len(batch))
	plan, err := n.svcCoordination.PlanDelegation(batch)
	if err != nil {
		n.log.Errorf(err.Error())
		return
	}
	var wg sync.WaitGroup
	var mutex sync.Mutex
	for k, v := range plan {
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
			}
			wg.Done()
		}(k, v.NodeAddr, v.Batch)
	}
	if _, ok := plan[n.id]; ok {
		// Persist
		persistResp, err := n.handlePersist(op, plan[n.id].Batch)
		if err != nil {
			n.log.Errorf(err.Error())
		}
		// Mark result
		mutex.Lock()
		for i, item := range plan[n.id].Batch {
			res[item.N] = persistResp[i]
		}
		mutex.Unlock()
		plan, err = n.svcCoordination.PlanPropagation(batch)
		for k, v := range plan {
			wg.Add(1)
			go func(nodeid string, addr string, batch entity.Batch) {
				// Propagate
				propResp, err := n.svcPropagation.Propagate(op, addr, batch)
				if err != nil {
					n.log.Errorf(err.Error())
				}
				// Parity check against master result
				for i, item := range batch {
					if persistResp[item.N] != propResp[i] {
						n.log.Warnf("Differing response master %s=%d, replica %s=%d", n.id, persistResp[item.N],
							nodeid, propResp[i])
					}
				}
				wg.Done()
			}(k, v.NodeAddr, v.Batch)
		}
	}
	wg.Wait()
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

// Receives GRPC call for propagation
func (n *node) Propagate(ctx context.Context, req *intapi.BatchOp) (reply *intapi.BatchReply, err error) {
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

func (n *node) Start() {
	go func() {
		for {
			select {
			case cat := <-n.initChan:
				n.svcPersistence.Init(cat, n.id)
				n.active = true
			}
		}
	}()
	n.svcDelegation.Start()
	n.svcPersistence.Start()
	n.svcPropagation.Start()
	n.svcCoordination.Start()
}

func (n *node) Stop() {
	n.svcCoordination.Stop()
	n.svcPersistence.Stop()
	n.svcPropagation.Stop()
	n.svcDelegation.Stop()
}
