package service

import (
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"

	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/log"
	"highvolume.io/shackle/repo"
)

type Persistence interface {
	Init(cat entity.Catalog, nodeID uint64) error
	Lock(batch entity.Batch) (res []uint8, err error)
	Rollback(batch entity.Batch) (res []uint8, err error)
	Commit(batch entity.Batch) (res []uint8, err error)
	Start()
	Stop()
}

type persistence struct {
	log           log.Logger
	clock         clock.Clock
	partitions    map[uint64]repo.Hash
	repos         map[uint64]repo.Hash
	repoFactory   repo.FactoryHash
	repoCfg       *config.RepoHash
	keyExp        time.Duration
	lockExp       time.Duration
	sweepInterval time.Duration
	repoMutex     sync.RWMutex
}

// NewPersistence returns a persistence service
func NewPersistence(cfg *config.App, log log.Logger, rf repo.FactoryHash) (r *persistence, err error) {
	return &persistence{
		log:           log,
		clock:         clock.New(),
		partitions:    map[uint64]repo.Hash{},
		repos:         map[uint64]repo.Hash{},
		repoFactory:   rf,
		repoCfg:       cfg.Repo.Hash,
		keyExp:        cfg.Repo.Hash.KeyExpiration,
		lockExp:       cfg.Repo.Hash.LockExpiration,
		sweepInterval: cfg.Repo.Hash.SweepInterval,
	}, nil
}

func (c *persistence) Init(cat entity.Catalog, hostID uint64) (err error) {
	c.repoMutex.Lock()
	defer c.repoMutex.Unlock()
	var hashRepo repo.Hash
	var clusterIDs = cat.GetHostPartitions(hostID)
	if clusterIDs == nil {
		err = fmt.Errorf("hostID not found: %d", hostID)
		return
	}
	for _, clusterID := range clusterIDs {
		hashRepo, err = c.repoFactory(c.repoCfg, clusterID)
		if err != nil {
			return
		}
		c.repos[clusterID] = hashRepo
		c.partitions[clusterID] = hashRepo
	}
	return
}

// Lock determines whether each hash has been seen and locks for processing
// Locks have a set expiration (default 30s). Items are unlocked after this timeout expires.
// Lock abandonment is measured and exposed as a metric.
func (c *persistence) Lock(batch entity.Batch) (res []uint8, err error) {
	res = make([]uint8, len(batch))
	var wg sync.WaitGroup
	var mutex sync.Mutex
	c.repoMutex.RLock()
	defer c.repoMutex.RUnlock()

	for k, batch := range batch.Partitioned() {
		wg.Add(1)
		go func(k uint64, batch entity.Batch) {
			hashRepo, ok := c.partitions[k]
			if !ok {
				err = fmt.Errorf("Partition not found %d", k)
				c.log.Errorf(err.Error())
				wg.Done()
				return
			}
			r1, err2 := hashRepo.Lock(batch)
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
func (c *persistence) Rollback(batch entity.Batch) (res []uint8, err error) {
	res = make([]uint8, len(batch))
	var wg sync.WaitGroup
	var mutex sync.Mutex
	c.repoMutex.RLock()
	defer c.repoMutex.RUnlock()

	for k, batch := range batch.Partitioned() {
		wg.Add(1)
		go func(k uint64, batch entity.Batch) {
			hashRepo, ok := c.partitions[k]
			if !ok {
				err = fmt.Errorf("Partition not found %d", k)
				c.log.Errorf(err.Error())
				wg.Done()
				return
			}
			r1, err2 := hashRepo.Rollback(batch)
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
func (c *persistence) Commit(batch entity.Batch) (res []uint8, err error) {
	res = make([]uint8, len(batch))
	var wg sync.WaitGroup
	var mutex sync.Mutex
	c.repoMutex.RLock()
	defer c.repoMutex.RUnlock()

	for k, batch := range batch.Partitioned() {
		wg.Add(1)
		go func(k uint64, batch entity.Batch) {
			hashRepo, ok := c.partitions[k]
			if !ok {
				err = fmt.Errorf("Partition not found %d", k)
				c.log.Errorf(err.Error())
				wg.Done()
				return
			}
			r1, err2 := hashRepo.Commit(batch)
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

// Start does nothing, Morty! It does nothing!
func (c *persistence) Start() {
}

// Close the repos
func (c *persistence) Stop() {
	for _, r := range c.repos {
		r.Close()
	}
}
