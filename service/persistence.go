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
	InitRepo(partition uint16) (err error)
	SyncRepo(partition uint16) (err error)
	MultiExec(partition uint16, ops []uint8, batches []entity.Batch) (res [][]uint8, err error)
	GetDatabases() []string
	Start()
	Stop()
}

type persistence struct {
	log           log.Logger
	clock         clock.Clock
	partitions    uint16
	repos         map[uint16]repo.Hash
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
		partitions:    cfg.Host.Partitions,
		repos:         map[uint16]repo.Hash{},
		repoFactory:   rf,
		repoCfg:       cfg.Repo.Hash,
		keyExp:        cfg.Repo.Hash.KeyExpiration,
		lockExp:       cfg.Repo.Hash.LockExpiration,
		sweepInterval: cfg.Repo.Hash.SweepInterval,
	}, nil
}

func (c *persistence) Init(cat entity.Catalog, hostID uint64) (err error) {
	var clusterIDs = cat.GetHostPartitions(hostID)
	if clusterIDs == nil {
		err = fmt.Errorf("hostID not found: %d", hostID)
		return
	}
	c.partitions = uint16(cat.Partitions)
	return
}

func (c *persistence) InitRepo(partition uint16) (err error) {
	if _, ok := c.repos[partition]; ok {
		return
	}
	hashRepo, err := c.repoFactory(c.repoCfg, c.partitions, partition)
	if err != nil {
		return
	}
	c.repoMutex.Lock()
	defer c.repoMutex.Unlock()
	c.repos[partition] = hashRepo
	return
}

func (c *persistence) SyncRepo(partition uint16) (err error) {
	hashRepo, ok := c.repos[partition]
	if !ok {
		err = fmt.Errorf("Partition not found %04x", partition)
		return
	}
	err = hashRepo.Sync()
	return
}

// GetDatabases returns a list of database names
func (c *persistence) GetDatabases() []string {
	return nil
}

func (c *persistence) MultiExec(partition uint16, ops []uint8, batches []entity.Batch) (res [][]uint8, err error) {
	c.repoMutex.RLock()
	defer c.repoMutex.RUnlock()
	hashRepo, ok := c.repos[partition]
	if !ok {
		err = fmt.Errorf("Partition not found %d", partition)
		c.log.Errorf(err.Error())
		return
	}
	res, err = hashRepo.MultiExec(ops, batches)
	if err != nil {
		c.log.Errorf(err.Error())
	}
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
