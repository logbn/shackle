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
	Close()
}

type persistence struct {
	clock      clock.Clock
	partitions int
	repos      map[int]repo.Hash
	log        log.Logger
}

// NewPersistence returns a persistence service
func NewPersistence(cfg *config.Hash, log log.Logger) (r *persistence, err error) {
	var (
		partitions = cfg.Partitions
		repos      = map[int]repo.Hash{}
	)
	for i := 0; i < partitions; i++ {
		repos[i], err = repo.NewHash(cfg, i)
		if err != nil {
			return
		}
	}

	return &persistence{clock.New(), partitions, repos, log}, nil
}

// Lock determines whether each hash has been seen and locks for processing
func (c persistence) Lock(batch entity.Batch) (res []int8, err error) {
	batches := make(map[int]entity.Batch)
	res = make([]int8, len(batch))
	for _, item := range batch {
		p := int(item.Hash[0]) % c.partitions
		if _, ok := batches[p]; !ok {
			batches[p] = entity.Batch{}
		}
		batches[p] = append(batches[p], item)
	}
	var wg sync.WaitGroup
	var mutex sync.Mutex
	for k, batch := range batches {
		wg.Add(1)
		go func(k int, batch entity.Batch) {
			r1, err := c.repos[k].Lock(batch)
			mutex.Lock()
			if err != nil {
				c.log.Debugf(err.Error())
				for _, item := range batch {
					res[item.N] = entity.LOCK_ERROR
				}
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
