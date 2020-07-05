package cluster

import (
	"io"

	dbsm "github.com/lni/dragonboat/v3/statemachine"

	"highvolume.io/shackle/config"
	"highvolume.io/shackle/log"
	"highvolume.io/shackle/service"
)

type MetaNode interface {
	Update([]byte) (dbsm.Result, error)
	Lookup(interface{}) (interface{}, error)
	SaveSnapshot(io.Writer, dbsm.ISnapshotFileCollection, <-chan struct{}) error
	RecoverFromSnapshot(io.Reader, []dbsm.SnapshotFile, <-chan struct{}) error
	Close() error
}

type metaNode struct {
	log             log.Logger
	clusterID       uint64
	nodeID          uint64
	keylen          int
	svcHash         service.Hash
	svcPersistence  service.Persistence
	svcDelegation   service.Delegation
	activeChan      chan bool
	active          bool
}

// NewMetaNode returns a new metaNode
func NewMetaNodeFactory(
	cfg config.App,
	log log.Logger,
	svcHash service.Hash,
	svcPersistence service.Persistence,
	svcDelegation service.Delegation,
) func(clusterID uint64, nodeID uint64) dbsm.IStateMachine {
	return func(clusterID uint64, nodeID uint64) dbsm.IStateMachine {
		return &metaNode{
			log,
			clusterID,
			nodeID,
			cfg.Host.KeyLength,
			svcHash,
			svcPersistence,
			svcDelegation,
			make(chan bool),
			false,
		}
	}
}

// Start starts services and initchan
func (n *metaNode) Start() {

}

// Stop stops services
func (n *metaNode) Stop() {

}

func (n *metaNode) Update(op []byte) (res dbsm.Result, err error) {
	n.log.Debugf("Update")
	return
}
func (n *metaNode) Lookup(interface{}) (res interface{}, err error) {
	n.log.Debugf("Lookup")
	return
}
func (n *metaNode) SaveSnapshot(w io.Writer, c dbsm.ISnapshotFileCollection, d <-chan struct{}) (err error) {
	n.log.Debugf("SaveSnapshot")
	return
}
func (n *metaNode) RecoverFromSnapshot(w io.Reader, c []dbsm.SnapshotFile, d <-chan struct{}) (err error) {
	n.log.Debugf("RecoverFromSnapshot")
	return
}

// Stop stops services
func (n *metaNode) Close() (err error) {
	return
}

// Active returns a channel that can be used to block the caller until the metaNode is active
func (n *metaNode) Active() chan bool {
	return n.activeChan
}
