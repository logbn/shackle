package mock

import (
	"crypto/sha1"
	"fmt"
	"sync"

	"highvolume.io/shackle/entity"
)

type ServicePersistence struct {
	Locks     int
	Rollbacks int
	Commits   int
	Starts    int
	Stops     int
	mutex     sync.Mutex
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
func (m *ServicePersistence) Start() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Starts++
	return
}
func (m *ServicePersistence) Stop() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.Stops++
	return
}

type ServiceHash struct{}

func (h *ServiceHash) Hash(a, b []byte) []byte {
	sha := sha1.Sum(append(a, b...))
	return sha[:]
}
