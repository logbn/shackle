package mock

import (
	"fmt"
	"sync"

	"highvolume.io/shackle/internal/entity"
)

type ServicePersistence struct {
	Locks             int
	Rollbacks         int
	Commits           int
	StartSweeperCalls int
	StopSweeperCalls  int
	Closes            int
	mutex             sync.Mutex
}

func (m *ServicePersistence) Lock(batch entity.Batch) (res []int8, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Locks++
	res = make([]int8, len(batch))
	for i := range batch {
		res[i] = entity.ITEM_LOCKED
	}
	if len(batch) == 7 {
		err = fmt.Errorf("test err")
	}
	return
}
func (m *ServicePersistence) Rollback(batch entity.Batch) (res []int8, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Rollbacks++
	res = make([]int8, len(batch))
	for i := range batch {
		res[i] = entity.ITEM_OPEN
	}
	if len(batch) == 7 {
		err = fmt.Errorf("test err")
	}
	return
}
func (m *ServicePersistence) Commit(batch entity.Batch) (res []int8, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Commits++
	res = make([]int8, len(batch))
	for i := range batch {
		res[i] = entity.ITEM_EXISTS
	}
	if len(batch) == 7 {
		err = fmt.Errorf("test err")
	}
	return
}
func (m *ServicePersistence) StartSweepers() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.StartSweeperCalls++
	return
}
func (m *ServicePersistence) StopSweepers() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.StopSweeperCalls++
	return
}
func (m *ServicePersistence) Close() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Closes++
	return
}
