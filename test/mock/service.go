package mock

import (
	"crypto/sha1"
	"fmt"
	"strings"
	"sync"

	"highvolume.io/shackle/entity"
)

// ServicePersistence
type ServicePersistence struct {
	metrics map[string]int
	mutex   sync.Mutex
}

// GetDatabases returns a list of database names
func (c *ServicePersistence) GetDatabases() []string {
	return []string{}
}

func (m *ServicePersistence) Init(cat entity.Catalog, nodeID uint64) (err error) {
	m.incr("Init")
	return
}
func (m *ServicePersistence) Lock(batch entity.Batch) (res []uint8, err error) {
	m.incr("Lock")
	res = make([]uint8, len(batch))
	for i, item := range batch {
		if strings.Contains(string(item.Hash), "ERROR") {
			res[i] = entity.ITEM_ERROR
			continue
		}
		res[i] = entity.ITEM_LOCKED
	}
	if len(batch) == 7 {
		err = fmt.Errorf("test err")
	}
	return
}
func (m *ServicePersistence) Rollback(batch entity.Batch) (res []uint8, err error) {
	m.incr("Rollback")
	res = make([]uint8, len(batch))
	for i, item := range batch {
		if strings.Contains(string(item.Hash), "ERROR") {
			res[i] = entity.ITEM_ERROR
			continue
		}
		res[i] = entity.ITEM_OPEN
	}
	if len(batch) == 7 {
		err = fmt.Errorf("test err")
	}
	return
}
func (m *ServicePersistence) Commit(batch entity.Batch) (res []uint8, err error) {
	m.incr("Commit")
	res = make([]uint8, len(batch))
	for i := range batch {
		res[i] = entity.ITEM_EXISTS
	}
	if len(batch) == 7 {
		err = fmt.Errorf("test err")
	}
	return
}
func (m *ServicePersistence) Start() {
	m.incr("Start")
	return
}
func (m *ServicePersistence) Stop() {
	m.incr("Stop")
	return
}
func (m *ServicePersistence) incr(metric string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.metrics == nil {
		m.metrics = map[string]int{}
	}
	m.metrics[metric]++
	return
}

// Thread safe metric retrieval
func (m *ServicePersistence) Count(metric string) (res int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	res, _ = m.metrics[metric]
	return
}

// ServiceHash
type ServiceHash struct{}

func (h *ServiceHash) Hash(a, b []byte) ([]byte, uint16) {
	sha := sha1.Sum(append(a, b...))
	return sha[:], uint16(0)
}
func (h *ServiceHash) GetPartition([]byte) uint16 {
	return uint16(0)
}

// ServiceCoordination
type ServiceCoordination struct {
	FuncJoin           func(id, addr string) (err error)
	FuncPlanDelegation func(entity.Batch) (b entity.BatchPlan, err error)
}

func (s *ServiceCoordination) Join(id, addr string) (err error) { return }
func (s *ServiceCoordination) PlanDelegation(batch entity.Batch) (b entity.BatchPlan, err error) {
	if s.FuncPlanDelegation == nil {
		return
	}
	return s.FuncPlanDelegation(batch)
}
func (s *ServiceCoordination) Start() (err error) { return }
func (s *ServiceCoordination) Stop()              { return }

func ServiceCoordinationFuncPlanDelegationBasic(batch entity.Batch) (bp entity.BatchPlan, err error) {
	if len(batch) == 0 {
		return
	}
	if strings.Contains(string(batch[0].Hash), "FATAL") {
		err = fmt.Errorf("FATAL")
		return
	}
	var b1 entity.Batch
	var b2 entity.Batch
	for i, item := range batch {
		if strings.Contains(string(item.Hash), "DELEGATE") {
			b2 = append(b2, item)
			b2[len(b2)-1].N = i
		} else {
			b1 = append(b1, item)
			b1[len(b1)-1].N = i
		}
	}
	bp = entity.BatchPlan{}
	if len(b1) > 0 {
		bp[1] = &entity.BatchPlanSegment{
			NodeAddr: "127.0.0.1:4708",
			Batch:    b1,
		}
	}
	if len(b2) > 0 {
		bp[2] = &entity.BatchPlanSegment{
			NodeAddr: "127.0.0.2:4708",
			Batch:    b2,
		}
	}
	return
}

// ServiceDelegation
type ServiceDelegation struct{}

func (s *ServiceDelegation) Delegate(op uint8, addr string, batch entity.Batch) (res []uint8, err error) {
	res = make([]uint8, len(batch))
	for i, item := range batch {
		if strings.Contains(string(item.Hash), "ERROR") {
			res[i] = entity.ITEM_ERROR
			continue
		}
		if strings.Contains(string(item.Hash), "DELEGATEFAIL") {
			for i := range res {
				res[i] = entity.ITEM_ERROR
			}
			err = fmt.Errorf("DELEGATEFAIL")
			return
		}
		switch op {
		case entity.OP_LOCK:
			res[i] = entity.ITEM_LOCKED
		case entity.OP_COMMIT:
			res[i] = entity.ITEM_EXISTS
		case entity.OP_ROLLBACK:
			res[i] = entity.ITEM_OPEN
		default:
			err = fmt.Errorf("Unrecognized operation %d", op)
		}
	}
	return
}
func (s *ServiceDelegation) Start() { return }
func (s *ServiceDelegation) Stop()  { return }
