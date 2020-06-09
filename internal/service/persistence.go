package service

import (
	"sync"

	"github.com/benbjohnson/clock"

	"highvolume.io/shackle/internal/config"
	"highvolume.io/shackle/internal/entity"
	"highvolume.io/shackle/internal/log"
	"highvolume.io/shackle/internal/repo"
)

type Persistence interface {
	Lock(batch entity.Batch) (res []int8, err error)
	Rollback(batch entity.Batch) (res []int8, err error)
	Commit(batch entity.Batch) (res []int8, err error)
	Close()
}

type persistence struct {
	clock      clock.Clock
	partitions int
	repos      map[int]repo.Hash
	log        log.Logger
}

// NewPersistence returns a persistence service
func NewPersistence(cfg *config.Hash, rfh repo.FactoryHash, log log.Logger) (r *persistence, err error) {
	var (
		partitions = cfg.Partitions
		repos      = map[int]repo.Hash{}
	)
	for i := 0; i < partitions; i++ {
		repos[i], err = rfh(cfg, i)
		if err != nil {
			return
		}
	}

	return &persistence{clock.New(), partitions, repos, log}, nil
}

// Lock determines whether each hash has been seen and locks for processing
// Locks have a set expiration (default 30s). Items are unlocked after this timeout expires.
// Lock abandonment is measured and exposed as a metric.
func (c persistence) Lock(batch entity.Batch) (res []int8, err error) {
	res = make([]int8, len(batch))
	var wg sync.WaitGroup
	var mutex sync.Mutex
	for k, batch := range batch.Partitioned(c.partitions) {
		wg.Add(1)
		go func(k int, batch entity.Batch) {
			r1, err2 := c.repos[k].Lock(batch)
			mutex.Lock()
			if err2 != nil {
				c.log.Errorf(err2.Error())
				for _, item := range batch {
					res[item.N] = entity.ITEM_ERROR
				}
				err = err2
			} else {
				for i, item := range batch {
					res[item.N] = r1[i]
				}
			}
			mutex.Unlock()
			wg.Done()
		}(k, batch)
	}
	wg.Wait()

	return
}

// Rollback determines whether each hash has been seen and locks for processing
func (c persistence) Rollback(batch entity.Batch) (res []int8, err error) {
	res = make([]int8, len(batch))
	var wg sync.WaitGroup
	var mutex sync.Mutex
	for k, batch := range batch.Partitioned(c.partitions) {
		wg.Add(1)
		go func(k int, batch entity.Batch) {
			r1, err2 := c.repos[k].Rollback(batch)
			mutex.Lock()
			if err2 != nil {
				c.log.Errorf(err2.Error())
				for _, item := range batch {
					res[item.N] = entity.ITEM_ERROR
				}
				err = err2
			} else {
				for i, item := range batch {
					res[item.N] = r1[i]
				}
			}
			mutex.Unlock()
			wg.Done()
		}(k, batch)
	}
	wg.Wait()

	return
}

// Commit will write hashes to the index and remove them from the lock if present.
// A commit will always succeed, regardless of whether the items are locked or by whom.
// A commit against an existing item will not indicate whether the item already existed.
// Commit volume against existing items is measured and exposed as a metric.
// The only way to read the state of an item is to acquire a lock.
func (c persistence) Commit(batch entity.Batch) (res []int8, err error) {
	res = make([]int8, len(batch))
	var wg sync.WaitGroup
	var mutex sync.Mutex
	for k, batch := range batch.Partitioned(c.partitions) {
		wg.Add(1)
		go func(k int, batch entity.Batch) {
			r1, err2 := c.repos[k].Commit(batch)
			mutex.Lock()
			if err2 != nil {
				c.log.Errorf(err2.Error())
				for _, item := range batch {
					res[item.N] = entity.ITEM_ERROR
				}
				err = err2
			} else {
				for i, item := range batch {
					res[item.N] = r1[i]
				}
			}
			mutex.Unlock()
			wg.Done()
		}(k, batch)
	}
	wg.Wait()

	return
}

// Close the repos
func (c persistence) Close() {
	for _, r := range c.repos {
		r.Close()
	}
}
