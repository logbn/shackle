package mock

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"logbin.io/shackle/config"
	"logbin.io/shackle/entity"
	"logbin.io/shackle/repo"
)

func RepoFactoryhash(cfg *config.RepoHash, partitions, id uint16) (r repo.Hash, err error) {
	if cfg.PathIndex == "error" {
		err = fmt.Errorf("error")
		return
	}
	r = &RepoHash{}
	return
}

type RepoHash struct {
	Closes           int
	Syncs            int
	mutex            sync.Mutex
	SweepExpiredFunc func(exp time.Time, limit int) (maxAge time.Duration, notFound, deleted int, err error)
	SweepLockedFunc  func(exp time.Time) (total int, deleted int, err error)
}

func (r *RepoHash) MultiExec(ops []uint8, batches []entity.Batch) (res [][]uint8, err error) {
	res = make([][]uint8, len(ops))
	for i := range ops {
		res[i], err = r.getRes(batches[i])
		if err != nil {
			return
		}
	}
	return
}
func (c *RepoHash) SweepExpired(exp time.Time, limit int) (maxAge time.Duration, notFound, deleted int, err error) {
	if c.SweepExpiredFunc != nil {
		return c.SweepExpiredFunc(exp, limit)
	}
	return
}
func (c *RepoHash) SweepLocked(exp time.Time) (total int, deleted int, err error) {
	if c.SweepLockedFunc != nil {
		return c.SweepLockedFunc(exp)
	}
	return
}
func (r *RepoHash) Sync() (err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.Syncs++
	return
}
func (r *RepoHash) Close() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.Closes++
}
func (r *RepoHash) getRes(batch entity.Batch) (res []uint8, err error) {
	res = make([]uint8, len(batch))
	for i, item := range batch {
		if strings.Contains(string(item.Hash), "EXISTS") {
			res[i] = entity.ITEM_EXISTS
		} else if strings.Contains(string(item.Hash), "OPEN") {
			res[i] = entity.ITEM_OPEN
		} else if strings.Contains(string(item.Hash), "LOCKED") {
			res[i] = entity.ITEM_LOCKED
		} else if strings.Contains(string(item.Hash), "BUSY") {
			res[i] = entity.ITEM_BUSY
		} else if strings.Contains(string(item.Hash), "ERROR") {
			res[i] = entity.ITEM_ERROR
		} else if strings.Contains(string(item.Hash), "FATAL") {
			err = fmt.Errorf(string(item.Hash))
			return
		}
	}
	return
}
