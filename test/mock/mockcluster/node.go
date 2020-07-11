package mockcluster

import (
	"io"

	dbsm "github.com/lni/dragonboat/v3/statemachine"

	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/service"
)

type Node struct {
	dbsm.IOnDiskStateMachine
	SvcPersistence service.Persistence
}

func (n *Node) HandleBatch(op uint8, batch entity.Batch) (res []uint8, err error) { return }
func (n *Node) Start() {
	n.SvcPersistence.Start()
}
func (n *Node) Stop() {
	n.SvcPersistence.Stop()
}

func (n *Node) Open(stopc <-chan struct{}) (res uint64, err error)               { return }
func (n *Node) Update([]dbsm.Entry) (res []dbsm.Entry, err error)                { return }
func (n *Node) Lookup(interface{}) (res interface{}, err error)                  { return }
func (n *Node) Sync() (err error)                                                { return }
func (n *Node) PrepareSnapshot() (res interface{}, err error)                    { return }
func (n *Node) SaveSnapshot(interface{}, io.Writer, <-chan struct{}) (err error) { return }
func (n *Node) RecoverFromSnapshot(io.Reader, <-chan struct{}) (err error)       { return }
func (n *Node) Close() (err error)                                               { return }
