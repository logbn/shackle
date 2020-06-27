package service

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/benbjohnson/clock"

	"highvolume.io/shackle/config"
	"highvolume.io/shackle/entity"
	"highvolume.io/shackle/log"
	"highvolume.io/shackle/repo"
)

type Persistence interface {
	Init(cat entity.ClusterCatalog, nodeID string) error
	Lock(batch entity.Batch) (res []int8, err error)
	Rollback(batch entity.Batch) (res []int8, err error)
	Commit(batch entity.Batch) (res []int8, err error)
	Start()
	Stop()
}

type persistence struct {
	log           log.Logger
	clock         clock.Clock
	partitions    map[uint16]repo.Hash
	repos         map[string]repo.Hash
	repoFactory   repo.FactoryHash
	repoCfg       *config.RepoHash
	keyExp        time.Duration
	lockExp       time.Duration
	sweepInterval time.Duration
	stopChan      chan bool
	repoMutex     sync.RWMutex
}

// NewPersistence returns a persistence service
func NewPersistence(cfg *config.App, log log.Logger, rf repo.FactoryHash) (r *persistence, err error) {
	return &persistence{
		log:           log,
		clock:         clock.New(),
		partitions:    map[uint16]repo.Hash{},
		repos:         map[string]repo.Hash{},
		repoFactory:   rf,
		repoCfg:       cfg.Repo.Hash,
		keyExp:        cfg.Repo.Hash.KeyExpiration,
		lockExp:       cfg.Repo.Hash.LockExpiration,
		sweepInterval: cfg.Repo.Hash.SweepInterval,
	}, nil
}

func (c *persistence) Init(cat entity.ClusterCatalog, nodeID string) (err error) {
	var node *entity.ClusterNode
	for _, n := range cat.Nodes {
		if n.ID == nodeID {
			node = &n
			break
		}
	}
	if node == nil {
		return fmt.Errorf("Node not found in catalog %s", nodeID)
	}
	c.repoMutex.Lock()
	defer c.repoMutex.Unlock()
	var (
		vnodeIDs        = map[int]string{}
		vnodePartitions = map[int][]uint16{}
	)
	for i, vn := range cat.VNodes {
		if vn.Node != nodeID {
			continue
		}
		vnodeIDs[i] = vn.ID
	}
	for _, p := range cat.Partitions {
		if _, ok := vnodeIDs[p.Master]; ok {
			vnodePartitions[p.Master] = append(vnodePartitions[p.Master], p.Prefix)
		}
		for _, i := range p.Replicas {
			if _, ok := vnodeIDs[i]; ok {
				vnodePartitions[i] = append(vnodePartitions[i], p.Prefix)
			}
		}
	}
	var r repo.Hash
	for i, id := range vnodeIDs {
		r, err = c.repoFactory(c.repoCfg, id, vnodePartitions[i])
		if err != nil {
			return
		}
		c.repos[id] = r
		for _, p := range vnodePartitions[i] {
			c.partitions[p] = r
		}
	}
	return
}

// Lock determines whether each hash has been seen and locks for processing
// Locks have a set expiration (default 30s). Items are unlocked after this timeout expires.
// Lock abandonment is measured and exposed as a metric.
func (c *persistence) Lock(batch entity.Batch) (res []int8, err error) {
	res = make([]int8, len(batch))
	var wg sync.WaitGroup
	var mutex sync.Mutex
	c.repoMutex.RLock()
	defer c.repoMutex.RUnlock()

	for k, batch := range batch.Partitioned() {
		wg.Add(1)
		go func(k uint16, batch entity.Batch) {
			r1, err2 := c.partitions[k].Lock(batch)
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
func (c *persistence) Rollback(batch entity.Batch) (res []int8, err error) {
	res = make([]int8, len(batch))
	var wg sync.WaitGroup
	var mutex sync.Mutex
	c.repoMutex.RLock()
	defer c.repoMutex.RUnlock()

	for k, batch := range batch.Partitioned() {
		wg.Add(1)
		go func(k uint16, batch entity.Batch) {
			r1, err2 := c.partitions[k].Rollback(batch)
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
func (c *persistence) Commit(batch entity.Batch) (res []int8, err error) {
	res = make([]int8, len(batch))
	var wg sync.WaitGroup
	var mutex sync.Mutex
	c.repoMutex.RLock()
	defer c.repoMutex.RUnlock()

	for k, batch := range batch.Partitioned() {
		wg.Add(1)
		go func(k uint16, batch entity.Batch) {
			r1, err2 := c.partitions[k].Commit(batch)
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

// Start starts background sweepers
func (c *persistence) Start() {
	if c.sweepInterval < 1 || c.stopChan != nil {
		return
	}
	c.stopChan = make(chan bool)
	var ticker = c.clock.Ticker(c.sweepInterval)
	go func() {
		var i int
		var t time.Time
		var keyexpTime time.Time
		var lockexpTime time.Time
		for {
			select {
			case <-ticker.C:
				c.repoMutex.RLock()
				// Using consistent timestamp across repo sweeps produces more uniform metrics
				t = c.clock.Now()
				keyexpTime = t.Add(-1 * c.keyExp)
				lockexpTime = t.Add(-1 * c.lockExp)
				// Iterating over a map randomly every tick results in noisy scan, deletion and abandonment metrics
				// Repos are sorted on every iteration (rather than once) because node shard inventory is dynamic
				var sorted = make([]string, len(c.repos))
				i = 0
				for k := range c.repos {
					sorted[i] = k
					i++
				}
				sort.Strings(sorted)
				for _, k := range sorted {
					if c.keyExp > 0 {
						// TODO - create expiration sweep limit oracle to perform sweep during periods of low traffic
						maxAge, notFound, deleted, err := c.repos[k].SweepExpired(keyexpTime, 0)
						if err != nil {
							// monitor error
							c.log.Error(err.Error())
						} else {
							// provide deleted and maxAge to expiration sweep limit oracle
							// monitor deletion rate
							_ = deleted
							// monitor maxage
							_ = maxAge
							// monitor notFound
							_ = notFound
						}
					}
					if c.lockExp > 0 {
						scanned, abandoned, err := c.repos[k].SweepLocked(lockexpTime)
						if err != nil {
							// monitor error
							c.log.Error(err.Error())
						} else {
							// provide scan rate to expiration sweep limit oracle
							// monitor scan rate
							_ = scanned
							// monitor abandonment
							_ = abandoned
						}
					}
				}
				c.repoMutex.RUnlock()
			case <-c.stopChan:
				ticker.Stop()
				return
			}
		}
	}()
	return
}

func (c *persistence) stopSweepers() {
	if c.stopChan != nil {
		c.stopChan <- true
		c.stopChan = nil
	}
}

// Close the repos
func (c *persistence) Stop() {
	c.stopSweepers()
	for _, r := range c.repos {
		r.Close()
	}
}
