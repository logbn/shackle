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

func (n *node) Active() bool {
	return n.active
}
func (n *node) Open(stopc <-chan struct{}) (res uint64, err error) {
	n.log.Debugf("[Host %d] Open %04x", n.cfg.ID, n.Partition)
	err = n.svcPersistence.InitRepo(n.Partition)
	if err != nil {
		fmt.Printf("%s", err.Error())
		return
	}
	return
}
func (n *node) Sync() (err error) {
	n.svcPersistence.SyncRepo(n.Partition)
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

// Update handles a replicated raft log
func (n *node) Update(ents []dbsm.Entry) ([]dbsm.Entry, error) {
	n.metricMutex.Lock()
	defer n.metricMutex.Unlock()
	n.updates++
	n.batches += len(ents)
	var batches []entity.Batch
	var ops []uint8
	var counts = make([]int, len(ents))
	var err error
	for i, e := range ents {
		if len(e.Cmd) == 0 {
			continue
		}
		// These batches of batches of batches are getting ridiculous.
		multiOps, multiBatches, err := entity.MultiBatchFromBytes(e.Cmd, n.keylen, n.svcHash)
		if err != nil {
			n.log.Errorf(err.Error())
		}
		batches = append(batches, multiBatches...)
		ops = append(ops, multiOps...)
		for j := range multiBatches {
			n.items += len(multiBatches[j])
		}
		counts[i] = len(multiOps)
	}
	res, err := n.svcPersistence.MultiExec(n.Partition, ops, batches)
	if err != nil {
		n.log.Errorf(err.Error())
		return ents, err
	}
	// Coalesce results
	var c int
	for i, count := range counts {
		ents[i].Result = dbsm.Result{}
		for j := 0; j < count; j++ {
			ents[i].Result.Data = append(ents[i].Result.Data, res[c]...)
			c++
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
