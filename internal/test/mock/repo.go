package mock

import (
	"fmt"
	"strings"
	"sync"

	"highvolume.io/shackle/internal/config"
	"highvolume.io/shackle/internal/entity"
	"highvolume.io/shackle/internal/repo"
)

func RepoFactoryhash(cfg *config.Hash, partition int) (r repo.Hash, err error) {
	r = &RepoHash{}
	return
}

type RepoHash struct {
	Closes int
	mutex  sync.Mutex
}

func (r *RepoHash) Lock(batch entity.Batch) (res []int8, err error) {
	return r.getRes(batch)
}
func (r *RepoHash) Rollback(batch entity.Batch) (res []int8, err error) {
	return r.getRes(batch)
}
func (r *RepoHash) Commit(batch entity.Batch) (res []int8, err error) {
	return r.getRes(batch)
}
func (r *RepoHash) Close() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.Closes++
}
func (r *RepoHash) getRes(batch entity.Batch) (res []int8, err error) {
	res = make([]int8, len(batch))
	for i, item := range batch {
		if strings.Contains(string(item.Hash), "EXISTS") {
			res[i] = entity.ITEM_EXISTS
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
