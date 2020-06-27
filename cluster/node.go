package cluster

import (
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
	log             log.Logger
	svcHash         service.Hash
	svcCoordination service.Coordination
	svcPersistence  service.Persistence
	svcPropagation  service.Propagation
	svcDelegation   service.Delegation
	initChan        chan entity.ClusterCatalog
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
		log,
		svcHash,
		svcCoordination,
		svcPersistence,
		svcPropagation,
		svcDelegation,
		initChan,
	}, nil
}

func (n *node) Lock(batch entity.Batch) (res []int8, err error) {
	// Get batch plan from coordination service
	// Parallel
	//   Send remote writes through delegation service
	//   Persist local writes & propagate replica writes
	// Merge results
	return n.svcPersistence.Lock(batch)
}

func (n *node) Commit(batch entity.Batch) (res []int8, err error) {
	// Get batch plan from coordination service
	// Parallel
	//   Send remote writes through delegation service
	//   Persist local writes & propagate replica writes
	// Merge results
	return n.svcPersistence.Commit(batch)
}

func (n *node) Rollback(batch entity.Batch) (res []int8, err error) {
	// Get batch plan from coordination service
	// Parallel
	//   Send remote writes through delegation service
	//   Persist local writes & propagate replica writes
	// Merge results
	return n.svcPersistence.Rollback(batch)
}

func (n *node) GetClusterManifest() (status entity.ClusterManifest, err error) {
	return n.svcCoordination.GetClusterManifest()
}

func (n *node) Start() {
	n.svcDelegation.Start()
	n.svcPersistence.Start()
	n.svcPropagation.Start()
	go func() {
		for {
			select {
			case cat := <-n.initChan:
				n.svcPersistence.Init(cat, n.id)
			}
		}
	}()
	n.svcCoordination.Start()
}

func (n *node) Stop() {
	n.svcCoordination.Stop()
	n.svcPersistence.Stop()
	n.svcPropagation.Stop()
	n.svcDelegation.Stop()
}
