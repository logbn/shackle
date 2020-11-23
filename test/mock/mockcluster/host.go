package mockcluster

import (
	dbsm "github.com/lni/dragonboat/v3/statemachine"

	"logbin.io/shackle/entity"
	"logbin.io/shackle/service"
)

type Host struct {
	dbsm.IStateMachine
	SvcPersistence service.Persistence
}

func (n *Host) Lock(batch entity.Batch) (res []uint8, err error) {
	r, err := n.SvcPersistence.MultiExec(0, []uint8{entity.OP_LOCK}, []entity.Batch{batch})
	return r[0], err
}
func (n *Host) Rollback(batch entity.Batch) (res []uint8, err error) {
	r, err := n.SvcPersistence.MultiExec(0, []uint8{entity.OP_ROLLBACK}, []entity.Batch{batch})
	return r[0], err
}
func (n *Host) Commit(batch entity.Batch) (res []uint8, err error) {
	r, err := n.SvcPersistence.MultiExec(0, []uint8{entity.OP_COMMIT}, []entity.Batch{batch})
	return r[0], err
}
func (n *Host) Start() (err error) {
	n.SvcPersistence.Start()
	return
}
func (n *Host) Stop() {
	n.SvcPersistence.Stop()
}
func (n *Host) Active() (c chan bool) {
	return
}

func (n *Host) PlanDelegation(batch entity.Batch) (plan entity.BatchPlan, err error) { return }

func (n *Host) GetInitChan() chan entity.Catalog { return make(chan entity.Catalog) }
