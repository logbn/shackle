package mockcluster

import (
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/service"
)

type Node struct {
	SvcPersistence service.Persistence
}

func (n *Node) Lock(batch entity.Batch) (res []int8, err error) {
	return n.SvcPersistence.Lock(batch)
}
func (n *Node) Rollback(batch entity.Batch) (res []int8, err error) {
	return n.SvcPersistence.Rollback(batch)
}
func (n *Node) Commit(batch entity.Batch) (res []int8, err error) {
	return n.SvcPersistence.Commit(batch)
}
func (n *Node) GetClusterManifest() (status *entity.ClusterManifest, err error) {
	return
}
func (n *Node) Start() {
	n.SvcPersistence.Start()
}
func (n *Node) Stop() {
	n.SvcPersistence.Stop()
}
